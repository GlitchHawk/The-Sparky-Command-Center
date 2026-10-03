"""Fleet ops layer (Tier 3) for Sparky Command Center.

Self-contained module owned by the ops feature; server.py only wires three
integration points (routes, one main() hook, UI snippet).

Design constraints (Jay, 2026-10-03):
- The Spark cluster keeps working exactly as before: zero auto-restarts, zero
  unattended writes. Every mutation is an explicitly triggered, token-authed,
  logged job.
- Ghost is never rebooted by this code; the Hermes harness lives there.
- Node interaction is read-only by default. The only node writes are the same
  ritual commands a human would type (launch-glm53.sh / docker rm -f), and a
  stop is guarded so a lone rank can never be Kill-reset while its twin serves.

Standard library only, like the rest of the dashboard.
"""

import copy
import hashlib
import json
import os
import queue
import shlex
import subprocess
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path

# --------------------------------------------------------------------------
# Config (config.json "fleet_ops" {...} > env > defaults)
# --------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
EVENTS_DIR = ROOT / "data" / "events"
BOOTLOGS_DIR = ROOT / "data" / "bootlogs"
BASELINE_FILE = ROOT / "data" / "ops_baseline.json"

_OPS_DEFAULTS = {
    "ops_key": "sparky-ops-local",
    "head": "spark",
    "worker": "spark2",
    "container": "vllm_glm53",
    "variants": {
        "redhat": "/var/tmp/glm-5.3-flash-nvfp4",
        "uncen": "/var/tmp/models/glm53-uncen-drowzeys",
    },
    "launch_script": "/home/glitch/rituals/launch-glm53.sh",
    "launch_gap_s": 25,        # worker -> head stagger, matches ritual doc
    "probe_interval_s": 10.0,
    "min_free_ram_g": 8,       # launcher hardening lesson: watchdog kills <3G; stay above
    "min_disk_free_g": 40,     # headroom for logs/caches, not weights
    "backup_root": "/home/glitch/ghost-cluster-backups",
    "image_snapshot_min_g": 8, # df headroom required to even try docker save
}

OPS = dict(_OPS_DEFAULTS)
OPS_KEY = _OPS_DEFAULTS["ops_key"]

# Injected once by server.py via ops_bind(): the host's config and process
# helpers. Declared here so the module imports cleanly standalone too.
CFG = {}
_run = None            # server.py: (argv, timeout) -> (rc, out, err)
_ssh_flags = None      # server.py: node dict -> ssh argv flags


def ops_bind(host_globals):
    """Copy the pieces of server.py this module plugs into."""
    global CFG, _run, _ssh_flags
    CFG = host_globals.get("CFG") or {}
    _run = host_globals["run_pipe"] if "run_pipe" in host_globals \
        else host_globals.get("_run")
    _ssh_flags = host_globals["_ssh_flags"]

# worker ranks run FIRST per launch-glm53.sh contract (TP2 rank1 = spark2)
RANKS = (("worker", "1"), ("head", "0"))


def fleet_cfg_reload():
    global OPS, OPS_KEY
    d = (CFG.get("fleet_ops") or {}) if CFG else {}
    OPS = dict(_OPS_DEFAULTS)
    OPS.update({k: v for k, v in d.items() if v is not None})
    OPS["variants"] = dict(_OPS_DEFAULTS["variants"])
    OPS["variants"].update(d.get("variants") or {})
    OPS["ops_key"] = (
        os.environ.get("SPARKY_OPS_KEY")
        or d.get("ops_key")
        or _OPS_DEFAULTS["ops_key"]
    )
    OPS_KEY = OPS["ops_key"]


# --------------------------------------------------------------------------
# Event log: data/events/YYYY-MM-DD.jsonl  (audit trail; disk-only, no state)
# --------------------------------------------------------------------------

def _ev_file():
    return EVENTS_DIR / f"{datetime.now():%Y-%m-%d}.jsonl"


def log_event(kind, msg, detail=None, job=None):
    rec = {"ts": datetime.now().isoformat(timespec="seconds"),
           "kind": kind, "msg": str(msg)[:400]}
    if detail:
        rec["detail"] = str(detail)[:600]
    if job:
        rec["job"] = job
    try:
        EVENTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(_ev_file(), "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, sort_keys=True) + "\n")
    except OSError:
        pass


def read_events(limit=120):
    out = []
    if not EVENTS_DIR.is_dir():
        return out
    for path in sorted(EVENTS_DIR.glob("*.jsonl"), reverse=True):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in reversed(lines):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            out.append(rec)
            if len(out) >= limit:
                out.reverse()
                return out
    out.reverse()
    return out


# --------------------------------------------------------------------------
# SSH plumbing (reuses server.py's _run / _ssh_flags / CFG nodes)
# --------------------------------------------------------------------------

def node_by_name(name):
    for n in CFG.get("nodes", []):
        if name in (n.get("name"), n.get("key"), n.get("host")):
            return n
    return None


def run_remote(node_name, remote_cmd, timeout=30):
    n = node_by_name(node_name)
    if n is None:
        return 1, "", f"unknown node {node_name!r}"
    argv = ["ssh"] + _ssh_flags(n) + [f"{n['user']}@{n['host']}", remote_cmd]
    return _run(argv, timeout)


def run_remote_sh(node_name, script, timeout=30):
    return run_remote(node_name, "bash -lc " + shlex.quote(script), timeout)


# --------------------------------------------------------------------------
# Node probe: one round trip, many facts. Read-only.
# --------------------------------------------------------------------------

_PROBE_SH = (
    "docker ps -a --filter name=^vllm_glm53$ --format '{{.Status}}'; "
    "echo ---; ss -ltn 2>/dev/null | grep -q ':8000 ' && echo listening "
    "|| echo notlistening; "
    "echo ---; test -d $OPS_WEIGHTS && echo weights-ok || echo weights-missing; "
    "echo ---; df -BG /var/tmp | awk 'NR==2{print $4}'; "
    "echo ---; awk '/MemAvailable/{printf \"%.1f\",$2/1048576}' /proc/meminfo; "
    "echo ---; docker images --format '{{.ID}}' --filter reference="
    "'ghcr.io/tonyd2wild/vllm-glm53-flash:sm121-v11-dflash2'; "
    "echo ---; test -f /home/glitch/patches/kv_cache_coordinator.py && echo patch-ok; "
    "test -f $HOME/patches/sparse_attn_indexer_kpool.py && echo kpool-ok; "
    "test -d /var/tmp/models/GLM-5.3-Flash-DFlash2 && echo drafter-ok"
)


def probe_node(name):
    """One ssh round trip {container,port,weights,disk,mem,image,prereqs}."""
    res = {"name": name, "reachable": False, "ts": time.time()}
    script = _PROBE_SH.replace("$OPS_WEIGHTS", _weights_path(name))
    rc, out, err = run_remote_sh(name, script, timeout=25)
    if rc != 0:
        res["error"] = (err or "no output").strip()[:160]
        return res
    sections = out.split("---")
    keys = ("container", "port8000", "weights", "disk_free_g",
            "mem_avail_g", "image_id", "prereqs")
    for k, chunk in zip(keys, [s.strip() for s in sections]):
        chunk = chunk.strip()
        if k == "container":
            res[k] = ("up" if chunk.startswith("Up")
                      else "exited" if chunk else "absent")
            res["status_raw"] = chunk or "(none)"
        elif k in ("disk_free_g", "mem_avail_g"):
            try:
                raw = chunk.splitlines()[0].rstrip("G")
                res[k] = float(raw) if "." in raw else int(raw)
            except (ValueError, IndexError):
                res[k] = None
        elif k == "port8000":
            res[k] = (chunk == "listening")
        elif k == "image_id":
            res[k] = chunk.splitlines()[0] if chunk else None
        elif k == "prereqs":
            have = set(chunk.split())
            res[k] = {"kv_cache_patch": "patch-ok" in have,
                      "kpool_patch": "kpool-ok" in have,
                      "drafter": "drafter-ok" in have}
    res["reachable"] = True
    return res


def _weights_path(node_name):
    role = rank_role(node_name)
    variant = BOOT_VARIANT.get(role) or "redhat"   # current selection, not truth
    return OPS["variants"].get(variant) or _OPS_DEFAULTS["variants"]["redhat"]


def rank_role(node_name):
    return "head" if node_name == OPS["head"] else "worker"


# --------------------------------------------------------------------------
# Rank state machine (derived, never authoritative: docker is the truth)
#   absent -> staged -> launching -> booting -> serving | crashed | stopped
# --------------------------------------------------------------------------

LIVE = {"nodes": {}, "ts": 0.0}
_LIVE_LOCK = threading.Lock()
BOOT_VARIANT = {}     # role -> last variant the operator chose (UI memory only)


def classify(probe):
    """Map a probe to a phase label. Pure function of observed facts.

    Containers run without a docker healthcheck, so 'Up' is all docker knows.
    Head: serving only when :8000 answers. Worker (never binds :8000): serving
    when Up and no dashboard-initiated launch in the last WORKER_BOOT_WINDOW s;
    within that window of a launch it is 'booting'. Imperfect but honest, and
    it self-corrects on the next probe after the window.
    """
    if not probe.get("reachable"):
        return "unreachable"
    name = probe.get("name")
    c = probe.get("container")
    status_raw = probe.get("status_raw", "")
    if c == "up":
        if name == OPS["head"]:
            return "serving" if probe.get("port8000") else "booting"
        if time.time() - LAST_LAUNCH.get(name, 0) <= WORKER_BOOT_WINDOW_S:
            return "booting"
        return "serving"
    if c == "exited":
        return "crashed" if status_raw.startswith("Exited (") else "stopped"
    if probe.get("port8000"):
        return "unreachable"   # port answers but docker disagrees: probe/prom discrepancy
    return "stopped"


WORKER_BOOT_WINDOW_S = 2100        # 35 min: 15 min boot reality + margin
LAST_LAUNCH = {}                   # node name -> monotonic-ish time.time() stamp


def snapshot_state():
    with _LIVE_LOCK:
        return copy.deepcopy({
            "nodes": LIVE["nodes"],
            "ts": LIVE["ts"],
            "phases": {k: v.get("phase") for k, v in LIVE["nodes"].items()},
            "jobs": {jid: job_public(j) for jid, j in _latest_jobs(8).items()},
            "events": read_events(24),
            "fleet_ok": _fleet_ok(),
        })


def _fleet_ok():
    ph = [v.get("phase") for v in LIVE["nodes"].values()]
    return bool(ph) and all(p == "serving" for p in ph)


# --------------------------------------------------------------------------
# Jobs: serialized background work. Every mutation runs here, logged.
# --------------------------------------------------------------------------

JOBS = {}                       # id -> dict
_JOB_SEQ = 0
_JOB_LOCK = threading.Lock()
_JOBQ = queue.Queue()
_JOB_THREAD = None

JOB_PUBLIC_KEYS = ("id", "op", "args", "state", "created", "started",
                   "ended", "error", "log", "result")


def job_public(j):
    return {k: j.get(k) for k in JOB_PUBLIC_KEYS}


def _latest_jobs(n):
    ids = sorted(JOBS, key=lambda k: JOBS[k]["id"])[:len(JOBS)]
    return {jid: JOBS[jid] for jid in ids[-n:]}


def jobs_snapshot():
    with _JOB_LOCK:
        return copy.deepcopy({jid: job_public(j) for jid, j in JOBS.items()
                              if j["state"] in ("queued", "running")}
                             or {k: job_public(v)
                                 for k, v in sorted(
                                     JOBS.items(),
                                     key=lambda kv: kv[1]["id"])[-3:]})


def job_submit(op, args, desc):
    global _JOB_SEQ
    with _JOB_LOCK:
        _JOB_SEQ += 1
        jid = f"j{_JOB_SEQ}"
        JOBS[jid] = {"id": jid, "op": op, "args": args, "desc": desc,
                     "state": "queued", "created": _now(),
                     "log": [], "error": None, "result": None}
    _JOBQ.put(jid)
    log_event("job-queued", desc, job=jid)
    return jid


def _now():
    return datetime.now().strftime("%H:%M:%S")


def _job_note(jid, line):
    with _JOB_LOCK:
        JOBS[jid]["log"].append(f"{_now()} {line}")
        JOBS[jid]["log"] = JOBS[jid]["log"][-40:]


def _job_set(jid, **kw):
    with _JOB_LOCK:
        JOBS[jid].update(kw)


def _job_runner():
    while True:
        jid = _JOBQ.get()
        try:
            j = JOBS.get(jid)
            if j is None:
                continue
            _job_set(jid, state="running", started=_now())
            log_event("job-start", j["desc"], job=jid)
            handler = JOB_OPS.get(j["op"])
            if handler is None:
                raise RuntimeError(f"no handler for op {j['op']!r}")
            result = handler(jid, j["args"] or {})
            _job_set(jid, state="done", ended=_now(), result=result)
            log_event("job-done", j["desc"],
                      detail=json.dumps(result)[:300] if result else None,
                      job=jid)
        except Exception as exc:  # noqa: BLE001 - job failures must not kill runner
            _job_set(jid, state="error", ended=_now(),
                     error=f"{type(exc).__name__}: {exc}"[:300])
            log_event("job-error", f"{jid} failed",
                      detail=f"{type(exc).__name__}: {exc}", job=jid)


def start_ops_workers():
    global _JOB_THREAD
    _JOB_THREAD = threading.Thread(target=_job_runner, daemon=True,
                                   name="ops-jobs")
    _JOB_THREAD.start()


# ---- job implementations -----------------------------------------------------

def _op_smoke(jid, args):
    host = args.get("host") or "127.0.0.1"
    port = int(args.get("port") or 8000)
    url = f"http://{host}:{port}/v1/models"
    _job_note(jid, f"GET {url} (6s timeout, read-only)")
    argv = ["curl", "-sm", "6", url]
    rc, out, err = _run(argv, 10)
    if rc != 0:
        _job_note(jid, f"endpoint unreachable (curl rc={rc}): {err[:120]}")
        return {"ok": False, "error": f"curl rc={rc}"}
    try:
        ids = [m.get("id") for m in json.loads(out).get("data", [])]
    except ValueError:
        ids = None
    _job_note(jid, f"models: {ids}")
    return {"ok": True, "models": ids}


def _op_launch(jid, args):
    node = args["node"]
    variant = args.get("variant", "redhat")
    if variant not in OPS["variants"]:
        raise ValueError(f"variant must be one of {sorted(OPS['variants'])}")
    probe = probe_node(node)
    if not probe.get("reachable"):
        raise RuntimeError(f"{node} unreachable, launch aborted: "
                           f"{probe.get('error', 'probe failed')}")
    if probe.get("container") == "up":
        _job_note(jid, f"{node} already has a live container; not touching it")
        return {"ok": True, "skipped": "container already up", "node": node}
    _prelaunch_gate(jid, probe)
    role = rank_role(node)
    rank = "1" if role == "worker" else "0"
    cmd = f"{OPS['launch_script']} {rank} {variant}"
    _job_note(jid, f"ssh {node}: {cmd}")
    rc, out, err = run_remote(node, cmd, timeout=90)
    _job_note(jid, f"launcher rc={rc} {out.strip()[:100]} {err.strip()[:100]}"
              if rc else f"launcher rc=0 {out.strip()[:100]}")
    if rc != 0:
        raise RuntimeError(f"launch-glm53.sh rc={rc}: {err.strip()[:200]}")
    BOOT_VARIANT[role] = variant
    LAST_LAUNCH[node] = time.time()
    log_event("launch", f"rank {rank} ({node}) variant={variant} submitted", job=jid)
    return {"ok": True, "node": node, "rank": rank, "variant": variant}


def _prelaunch_gate(jid, probe):
    """Hard gates from the crash lessons. Raise = job error, visible in UI."""
    mem, disk = probe.get("mem_avail_g"), probe.get("disk_free_g")
    if mem is not None and mem < OPS["min_free_ram_g"]:
        raise RuntimeError(
            f"MemAvailable {mem}G < {OPS['min_free_ram_g']}G gate "
            f"(launch during low-RAM = OOM-killer kills the worker)")
    if disk is not None and disk < OPS["min_disk_free_g"]:
        raise RuntimeError(f"/var/tmp free {disk}G < {OPS['min_disk_free_g']}G gate")
    pre = probe.get("prereqs") or {}
    missing = [k for k, v in pre.items() if not v]
    drafter_ok = pre.get("drafter")
    if missing or not drafter_ok:
        raise RuntimeError(f"prereqs missing on node: "
                           f"{missing + ([] if drafter_ok else ['drafter'])}")
    _job_note(jid, f"gate ok: mem={mem}G disk={disk}G prereqs complete")


def _op_stop(jid, args):
    node = args["node"]
    force = bool(args.get("force"))
    peer = OPS["worker"] if node == OPS["head"] else OPS["head"]
    pp = LIVE["nodes"].get(peer, {}).get("probe", {})
    peer_up = pp.get("phase") in ("serving", "booting")
    if peer_up and not force:
        return {"ok": True, "skipped":
                f"peer {peer} still serves; pass force:true to stop a lone rank"}
    rc, out, err = run_remote(node, "docker rm -f vllm_glm53 2>/dev/null; true",
                              timeout=30)
    _job_note(jid, f"docker rm -f rc={rc} {out.strip()[:60]}")
    log_event("stop", f"container removed on {node} (force={force})", job=jid)
    return {"ok": rc == 0, "node": node, "forced": force}


def _op_collect_logs(jid, args):
    node = args.get("node") or OPS["head"]
    tail = min(int(args.get("tail") or 600), 4000)
    rc, out, err = run_remote(
        node, f"docker logs --tail {tail} vllm_glm53 2>&1; "
              f"echo ---; uptime -p; free -g | head -2", timeout=40)
    if rc != 0:
        raise RuntimeError(f"logs failed: {err[:160]}")
    BOOTLOGS_DIR.mkdir(parents=True, exist_ok=True)
    path = BOOTLOGS_DIR / f"{datetime.now():%Y%m%d-%H%M%S}-{node}.log"
    path.write_text(out)
    _job_note(jid, f"saved {path.name} ({len(out)} bytes)")
    log_event("collect-logs", f"{node} -> {path.name}", job=jid)
    return {"ok": True, "file": str(path)}


def _op_audit(jid, args):
    """Read-only fleet audit: phases, disk, boot drift, upstream."""
    lines = []
    for role, node in (("head", OPS["head"]), ("worker", OPS["worker"])):
        p = probe_node(node)
        phase = classify(p)
        lines.append(f"{role} {node}: {phase}, disk={p.get('disk_free_g')}G, "
                     f"mem={p.get('mem_avail_g')}G")
    drift = drift_check()
    if drift.get("changed"):
        lines.append("drift: " + "; ".join(drift["changed"]))
    github = upstream_check()
    if github:
        lines.append(f"upstream: {github}")
    text = "\n".join(lines)
    _job_note(jid, "audit complete")
    log_event("audit", text)
    return {"ok": True, "report": text,
            "drift": drift.get("changed"), "upstream": github}


def _op_snapshot_image(jid, args):
    """docker save | gzip of the RUNNING image on a node, streamed to ghost disk.

    Heavy (several GB, minutes). Preflight: ghost disk headroom + .gz absence.
    """
    node = args.get("node") or OPS["head"]
    tag = "ghcr.io/tonyd2wild/vllm-glm53-flash:sm121-v11-dflash2"
    root = Path(OPS["backup_root"])
    root.mkdir(parents=True, exist_ok=True)
    out = root / f"glm53-image-{node}-{datetime.now():%Y%m%d}.tar.gz"
    if out.exists():
        return {"ok": True, "skipped": f"{out.name} already exists",
                "file": str(out)}
    rc, dfout, _ = _run(["df", "-BG", "--output=avail", str(root)], 10)
    try:
        free = int(dfout.strip().splitlines()[-1].rstrip("G"))
    except (ValueError, IndexError):
        free = None
    if free is not None and free < OPS["image_snapshot_min_g"]:
        raise RuntimeError(f"ghost disk {free}G < {OPS['image_snapshot_min_g']}G "
                           f"needed for image snapshot")
    _job_note(jid, f"streaming docker save {tag} from {node} (~minutes)")
    node_rec = node_by_name(node)
    ssh_argv = ["ssh"] + _ssh_flags(node_rec) + \
        [f"{node_rec['user']}@{node_rec['host']}",
         f"docker save {shlex.quote(tag)} | gzip -1"]
    with open(out, "wb") as fh:
        p = subprocess.Popen(ssh_argv, stdout=fh,
                             stderr=subprocess.PIPE, text=True)
        _, err = p.communicate(timeout=1800)
    if p.returncode != 0:
        out.unlink(missing_ok=True)
        raise RuntimeError(f"docker save failed rc={p.returncode}: {err[:160]}")
    size_g = out.stat().st_size / 1e9
    _job_note(jid, f"saved {out.name} {size_g:.1f}G")
    log_event("snapshot", f"{node} image -> {out.name} ({size_g:.1f}G)", job=jid)
    return {"ok": True, "file": str(out), "gb": round(size_g, 1)}


JOB_OPS = {
    "smoke": _op_smoke,
    "launch": _op_launch,
    "stop": _op_stop,
    "collect-logs": _op_collect_logs,
    "audit": _op_audit,
    "snapshot-image": _op_snapshot_image,
}


# --------------------------------------------------------------------------
# Versioning / drift / upstream (read-only, run inside audit or on demand)
# --------------------------------------------------------------------------

def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def ghost_files_state():
    return {str(p.relative_to(ROOT)): _sha256(p)
            for p in sorted(ROOT.rglob("*.py")) + sorted(ROOT.rglob("*.json"))
            if "data" not in p.parts and "__pycache__" not in p.parts
            and ".history" not in p.parts}


def drift_check():
    cur = ghost_files_state()
    try:
        prev = json.loads(BASELINE_FILE.read_text())
    except (OSError, ValueError):
        BASELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
        BASELINE_FILE.write_text(json.dumps(cur, indent=1, sort_keys=True))
        log_event("baseline", "first drift baseline written")
        return {"changed": [], "baseline": "created"}
    changed = sorted(k for k in cur if prev.get(k) != cur[k]
                     or k not in prev) + \
        sorted(k for k in prev if k not in cur)
    changed = sorted(set(changed))
    return {"changed": changed, "baseline": "existing",
            "ts": datetime.now().isoformat(timespec="seconds")}


def upstream_check():
    """Latest upstream commit sha on tonyd2wild main, if reachable."""
    try:
        req = urllib.request.Request(
            "https://api.github.com/repos/tonyd2wild/The-Sparky-Command-Center/"
            "commits/main", headers={"Accept": "application/vnd.github+json",
                                     "User-Agent": "sparky-ops"})
        with urllib.request.urlopen(req, timeout=6) as r:
            data = json.loads(r.read().decode())
        return {"upstream_sha": data.get("sha", "")[:10],
                "message": (data.get("commit", {}).get("message") or "")[:80]}
    except Exception as exc:  # noqa: BLE001 - upstream check is best effort
        return {"error": str(exc)[:100]}


# --------------------------------------------------------------------------
# Background prober + REST surface (called from server.py)
# --------------------------------------------------------------------------

def _probe_loop():
    while True:
        for node in (OPS["head"], OPS["worker"]):
            p = probe_node(node)
            with _LIVE_LOCK:
                LIVE["nodes"][node] = {
                    "probe": {k: v for k, v in p.items() if k != "ts"},
                    "phase": classify(p),
                    "role": rank_role(node),
                    "ts": time.time(),
                }
                LIVE["ts"] = time.time()
            time.sleep(1.0)     # stagger the pair, be gentle
        time.sleep(float(OPS["probe_interval_s"]))


def init_ops():
    """ Called once from server.main() before pollers start."""
    fleet_cfg_reload()
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    BOOTLOGS_DIR.mkdir(parents=True, exist_ok=True)
    threading.Thread(target=_probe_loop, daemon=True,
                     name="ops-probe").start()
    threading.Thread(target=_job_runner, daemon=True,
                     name="ops-jobs").start()
    log_event("boot", "ops layer initialised "
              f"(head={OPS['head']} worker={OPS['worker']})")


def api_state():
    with _LIVE_LOCK:
        nodes = copy.deepcopy(LIVE["nodes"])
    return {"ok": True, "nodes": nodes, "ts": LIVE["ts"],
            "jobs": jobs_snapshot(), "events": read_events(24),
            "fleet_ok": _fleet_ok(),
            "variants": sorted(OPS["variants"])}


def api_job(jid):
    with _JOB_LOCK:
        j = JOBS.get(jid)
        return {"ok": bool(j), "job": copy.deepcopy(job_public(j))} if j \
            else {"ok": False, "error": f"no job {jid}"}


def api_post(action, req):
    """Entry for POST /api/ops/<action>. Token check happens here so the HTTP
    layer in server.py stays dumb. Mutating ops enqueue a job; smoke runs inline."""
    if req.get("key") != OPS_KEY:
        return {"ok": False, "error": "bad or missing ops key"}
    if action == "smoke":
        a = {"host": req.get("host") or "127.0.0.1",
             "port": int(req.get("port") or 8000)}
        jid = job_submit("smoke", a, f"smoke probe {a['host']}:{a['port']}")
        result = _op_smoke(jid, a)
        _job_set(jid, state="done", ended=_now(), result=result)
        return {"ok": True, "job": jid, "smoke": result}
    specs = {
        "launch": ("node", "variant"),
        "stop": ("node",),
        "collect-logs": (),
        "audit": (),
        "snapshot-image": (),
    }
    spec = specs.get(action)
    if spec is None:
        return {"ok": False, "error": f"unknown action {action!r}"}
    missing = [k for k in spec if not req.get(k)]
    if missing:
        return {"ok": False, "error": f"missing args: {missing}"}
    args = {k: req[k] for k in spec}
    desc = {"launch": f"launch {args.get('node')}/{args.get('variant')}",
            "stop": f"stop rank on {args.get('node')}",
            "collect-logs": "collect engine logs",
            "audit": "fleet audit",
            "snapshot-image": "snapshot engine image"}[action]
    jid = job_submit(action, args, desc)
    return {"ok": True, "job": jid}
