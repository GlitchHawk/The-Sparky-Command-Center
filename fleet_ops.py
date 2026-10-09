"""Fleet ops layer (Tier 3) for Sparky Command Center.

Self-contained module owned by the ops feature; server.py only wires three
integration points (routes, one main() hook, UI snippet).

Design constraints (Jay, 2026-10-03):
- The Spark cluster keeps working exactly as before: zero auto-restarts, zero
  unattended writes. Every mutation is an explicitly triggered, token-authed,
  logged job.
- Ghost is never rebooted by this code; the Hermes harness lives there.
- Node interaction is read-only by default. Node writes are the same ritual
  commands a human would type, and a stop is guarded so a lone rank can never
  be torn down while its twin serves.

2026-10-09 (Flash cutover): the cluster runs the knapcio TP2 stack
(~/glm53-knapcio/start_tp2.sh + env.tp2-jay, containers glm53a-r0/r1, local
image glm53-roce:v11-b58f34ea, weights glm-quant-mix-lossless8 = "Flash").
Launch is head-driven: the launcher's `serve` runs ONCE on the head and brings
up worker (rank 1) then head (rank 0) itself. Stops use the launcher's
stop_preserving.py (docker stop, container kept for re-inspection) - the old
docker rm -f per-rank contract survives only as a fallback when no
launch_script is configured. Container base name, image tag, drafter dir and
prereq files are config-driven (config.json "fleet_ops"), defaulting to the
active Flash stack.

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
    "container": "glm53a",  # container base name; live ranks are glm53a-r0 / glm53a-r1
    "variants": {
        "flash": "/var/tmp/models/glm-quant-mix-lossless8-nvidia/lossless8",
    },
    "launch_script": "/home/glitch/glm53-knapcio/start_tp2.sh",
    "launch_env_file": "env.tp2-jay",
    "launch_gap_s": 25,        # legacy per-rank path only; head-driven serve ignores it
    "probe_interval_s": 10.0,
    "min_free_ram_g": 8,       # launcher's own RAM gates make this an early warning only
    "min_disk_free_g": 40,     # headroom for logs/caches, not weights
    "backup_root": "/home/glitch/ghost-cluster-backups",
    "image_snapshot_min_g": 8, # df headroom required to even try docker save
    "image_tag": "glm53-roce:v11-b58f34ea",
    "drafter_dir": "/var/tmp/models/GLM-5.3-Flash-DFlash2-fp8blk",
    "prereq_files": ["kv_cache_coordinator.py", "sparse_attn_indexer_kpool.py"],
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
    "docker ps -a --filter name=^$CTN_BASE- "
    "--format '{{.Names}}|{{.Status}}|{{.Image}}'; "
    "echo ---; ss -ltn 2>/dev/null | grep -q ':8000 ' && echo listening "
    "|| echo notlistening; "
    "echo ---; test -d $WEIGHTS && echo weights-ok || echo weights-missing; "
    "test -d $DRAFTER && echo drafter-ok || echo drafter-missing; "
    "echo ---; df -BG /var/tmp | awk 'NR==2{print $4}'; "
    "echo ---; awk '/MemAvailable/{printf \"%.1f\",$2/1048576}' /proc/meminfo; "
    "echo ---; docker images --format '{{.ID}}' --filter reference='$IMAGE_TAG'; "
    "echo ---; test -f $ENVFILE && echo envfile-ok || echo envfile-missing; "
    "test -d $OVERLAYDIR && echo overlay-ok || echo overlay-missing"
)


def probe_node(name):
    """One ssh round trip {ranks,container,port8000,paths,disk,mem,image,prereqs}."""
    res = {"name": name, "reachable": False, "ts": time.time()}
    script = (_PROBE_SH
              .replace("$CTN_BASE", OPS["container"])
              .replace("$WEIGHTS", _flash_weights())
              .replace("$DRAFTER", OPS["drafter_dir"])
              .replace("$IMAGE_TAG", OPS["image_tag"])
              .replace("$ENVFILE", _launch_env_abs())
              .replace("$OVERLAYDIR", _launch_root()))
    rc, out, err = run_remote_sh(name, script, timeout=25)
    if rc != 0:
        res["error"] = (err or "no output").strip()[:160]
        return res
    sections = out.split("---")
    keys = ("ranks_raw", "port8000", "paths", "disk_free_g",
            "mem_avail_g", "image_id", "prereqs")
    for k, chunk in zip(keys, [s.strip() for s in sections]):
        chunk = chunk.strip()
        if k == "ranks_raw":
            ranks = {}
            for line in chunk.splitlines():
                nm, st, img = (line.split("|") + ["", "", ""])[:3]
                nm = nm.strip()
                if not nm:
                    continue
                base = OPS["container"] + "-"
                rank = nm[len(base):] if nm.startswith(base) else "?"
                if rank.startswith("r") and rank[1:].isdigit():
                    rank = rank[1:]     # glm53a-r0 -> "0" (knapcio rank suffix)
                ranks[rank] = {"name": nm, "status_raw": st.strip(),
                               "image": img.strip()}
            res["ranks"] = ranks
            # Container verdict follows the head rank (0) when present, else
            # the first rank seen; ranks of other bases cannot appear because
            # the docker filter is anchored on the configured base name.
            pick = ranks.get("0") or (ranks[next(iter(ranks))] if ranks else None)
            if pick:
                st = pick["status_raw"]
                res["container"] = ("up" if st.startswith("Up")
                                    else "exited" if st else "absent")
                res["status_raw"] = st or "(none)"
                res["container_name"] = pick["name"]
            else:
                res["container"] = "absent"
                res["status_raw"] = "(none)"
        elif k in ("disk_free_g", "mem_avail_g"):
            try:
                raw = chunk.splitlines()[0].rstrip("G")
                res[k] = float(raw) if "." in raw else int(raw)
            except (ValueError, IndexError):
                res[k] = None
        elif k == "port8000":
            res[k] = (chunk == "listening")
        elif k == "paths":
            have = set(chunk.split())
            res["weights_ok"] = "weights-ok" in have
            res["drafter_ok"] = "drafter-ok" in have
        elif k == "image_id":
            res[k] = chunk.splitlines()[0] if chunk else None
        elif k == "prereqs":
            have = set(chunk.split())
            pr = {"overlay": "overlay-ok" in have}
            if rank_role(name) == "head":
                # env.tp2-jay lives only on the head (not part of the synced
                # overlay), so it is a head-only prereq.
                pr["envfile"] = "envfile-ok" in have
            res[k] = pr
    res["reachable"] = True
    return res


def _flash_weights():
    """Active weights dir (Flash). UI-selected variants are history only."""
    return next(iter(OPS["variants"].values()))


def _launch_root():
    """Dir holding launch_script; env file and overlay live beside it."""
    return os.path.dirname(OPS["launch_script"]) or "."


def _launch_env_abs():
    env = OPS.get("launch_env_file") or ""
    if not env:
        return ""
    return env if os.path.isabs(env) else os.path.join(_launch_root(), env)


def rank_role(node_name):
    return "head" if node_name == OPS["head"] else "worker"


# --------------------------------------------------------------------------
# Rank state machine (derived, never authoritative: docker is the truth)
#   absent -> staged -> launching -> booting -> serving | crashed | stopped
# --------------------------------------------------------------------------

LIVE = {"nodes": {}, "ts": 0.0}
_LIVE_LOCK = threading.Lock()


def classify(probe):
    """Map a probe to a phase label. Pure function of observed facts.

    Containers run without a docker healthcheck, so 'Up' is all docker knows.
    Head: serving only when :8000 answers. Worker (never binds :8000): serving
    when Up and no dashboard-initiated fleet serve in the last
    WORKER_BOOT_WINDOW s; within that window of a serve it is 'booting'.
    Imperfect but honest, and it self-corrects on the next probe after the
    window. Phases are per node; the fleet-wide verdict lives in
    _fleet_ok() ('serving' everywhere, or head serving + worker Up inside the
    serve boot window).
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


WORKER_BOOT_WINDOW_S = 4200        # 70 min: covers the whole fleet boot window
LAST_LAUNCH = {}                   # node name -> time.time() stamp (serve jobs)


def fleet_serving():
    """True while a serve-driven boot is still within its boot window."""
    return any(time.time() - t0 <= WORKER_BOOT_WINDOW_S
               for t0 in LAST_LAUNCH.values())


def snapshot_state():
    with _LIVE_LOCK:
        return copy.deepcopy({
            "nodes": LIVE["nodes"],
            "ts": LIVE["ts"],
            "phases": {k: v.get("phase") for k, v in LIVE["nodes"].items()},
            "jobs": {jid: job_public(j) for jid, j in _latest_jobs(8).items()},
            "events": read_events(24),
            "fleet_ok": _fleet_ok(),
            "launcher": {"script": OPS.get("launch_script") or "",
                         "env_file": _launch_env_abs(),
                         "container": OPS.get("container") or "",
                         "image_tag": OPS.get("image_tag") or "",
                         "variant": _flash_weights(),
                         "variants": sorted(OPS["variants"])},
        })


def _fleet_ok():
    ph = {n: v.get("phase") for n, v in LIVE["nodes"].items()}
    if not ph:
        return False
    if all(p == "serving" for p in ph.values()):
        return True
    # Serve boot window: head already serves while the worker rank is still
    # inside the boot window (up, booting) rather than labelled crashed/stopped.
    return (fleet_serving()
            and ph.get(OPS["head"]) == "serving"
            and ph.get(OPS["worker"]) in ("serving", "booting"))


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
    head = node_by_name(OPS["head"]) or {}
    # The engine endpoint lives on the head rank of the TP2 pair; the node arg
    # selects the UI context only. The probe itself always targets the head.
    host = args.get("host") or head.get("host") or "127.0.0.1"
    port = int(args.get("port") or 8000)
    url = f"http://{host}:{port}/v1/models"
    _job_note(jid, f"GET {url} (6s timeout, read-only)")
    argv = ["curl", "-sm", "6", url]
    rc, out, err = _run(argv, 10)
    if rc != 0:
        _job_note(jid, f"endpoint unreachable (curl rc={rc})")
        return {"ok": False, "error": f"curl rc={rc}: no engine on {host}:{port}"}
    try:
        ids = [m.get("id") for m in json.loads(out).get("data", [])]
    except ValueError:
        ids = None
    _job_note(jid, f"models: {ids}")
    return {"ok": True, "endpoint": f"{host}:{port}", "models": ids}


def _op_launch(jid, args):
    """Head-driven fleet serve via the knapcio launcher.

    Runs ONCE on the head node: `ENV_FILE=<env> bash start_tp2.sh serve`; the
    launcher itself starts worker (rank 1) then head (rank 0). Its own
    preflight refuses to collide with an existing rank container, so the only
    dashboard-side guard needed is "fleet already Up -> skip". On submit,
    LAST_LAUNCH is stamped for both nodes so classify() reports booting
    instead of crashed during the boot window.
    """
    variant = args.get("variant", "flash")
    if variant not in OPS["variants"]:
        raise ValueError(f"variant must be one of {sorted(OPS['variants'])}")
    env_abs = _launch_env_abs()
    if not env_abs:
        raise RuntimeError("no launch_env_file configured (set "
                           "fleet_ops.launch_env_file in config.json)")
    probed = probe_node(OPS["head"])
    if not probed.get("reachable"):
        raise RuntimeError(f"head {OPS['head']} unreachable, serve aborted: "
                           f"{probed.get('error', 'probe failed')}")
    ranks = probed.get("ranks") or {}
    live = [d["name"] for d in ranks.values()
            if (d.get("status_raw") or "").startswith("Up")]
    if live:
        _job_note(jid, f"fleet already live ({', '.join(live)}); not touching it")
        return {"ok": True, "skipped": "containers already up", "ranks": live}
    _prelaunch_gate(jid, probed)
    script = os.path.basename(OPS["launch_script"])
    cmd = (f"cd {_launch_root()} && ENV_FILE={shlex.quote(env_abs)} "
           f"bash {shlex.quote(script)} serve")
    _job_note(jid, f"ssh {OPS['head']}: {cmd}")
    rc, out, err = run_remote(OPS["head"], cmd, timeout=240)
    _job_note(jid, f"launcher rc={rc} {out.strip()[:100]} {err.strip()[:100]}"
              if rc else f"launcher rc=0 {out.strip()[:100]}")
    if rc != 0:
        raise RuntimeError(f"start_tp2.sh serve rc={rc}: {err.strip()[:200]}")
    for n in (OPS["worker"], OPS["head"]):
        LAST_LAUNCH[n] = time.time()
    log_event("launch", f"fleet serve submitted via {script} (env={env_abs})",
              job=jid)
    return {"ok": True, "op": "serve", "head": OPS["head"],
            "worker": OPS["worker"], "variant": variant}


def _prelaunch_gate(jid, probe):
    """Gates before a fleet serve, evaluated on the head probe. By this point
    no fleet container is Up, so host RAM should be mostly free - a low
    MemAvailable means something else is holding RAM and we refuse rather
    than serve into a contested host (the launcher + earlyoom own the hard
    limits at boot)."""
    mem, disk = probe.get("mem_avail_g"), probe.get("disk_free_g")
    if mem is not None and mem < OPS["min_free_ram_g"]:
        raise RuntimeError(
            f"MemAvailable {mem}G < {OPS['min_free_ram_g']}G gate with the "
            f"fleet down - something else is holding RAM")
    if disk is not None and disk < OPS["min_disk_free_g"]:
        raise RuntimeError(f"/var/tmp free {disk}G < {OPS['min_disk_free_g']}G gate")
    missing = [k for k, v in (probe.get("prereqs") or {}).items() if not v]
    if not probe.get("drafter_ok"):
        missing.append("drafter")
    if missing:
        raise RuntimeError(f"head prereqs missing: {missing}")
    _job_note(jid, f"gate ok: mem={mem}G disk={disk}G prereqs complete")


def _op_stop(jid, args):
    """Stop ONE rank via the launcher's stop_preserving.py (never removes:
    containers stay for inspection / fleet-start). Guarded: refuses while the
    peer rank serves, unless force:true - a lone surviving rank is useless."""
    node = args["node"]
    force = bool(args.get("force"))
    peer = OPS["worker"] if node == OPS["head"] else OPS["head"]
    pp = LIVE["nodes"].get(peer, {}).get("probe", {})
    peer_up = pp.get("phase") in ("serving", "booting")
    if peer_up and not force:
        return {"ok": True, "skipped":
                f"peer {peer} still serves; pass force:true to stop a lone rank"}
    rc, out, err = run_remote_sh(
        node,
        f"python3 {_launch_root()}/scripts/stop_preserving.py "
        f"--root {_launch_root()} --container "
        f"{shlex.quote(_rank_container(node))}",
        timeout=60)
    _job_note(jid, f"stop_preserving rc={rc} {out.strip()[:80]} {err.strip()[:80]}")
    log_event("stop", f"{_rank_container(node)} stopped on {node} "
              f"(preserved, force={force})", job=jid)
    return {"ok": rc == 0, "node": node, "container": _rank_container(node),
            "forced": force}


def _rank_container(node_name):
    """Rank container name for a config node, from the live probe if present."""
    p = LIVE["nodes"].get(node_name, {}).get("probe", {})
    if p.get("container_name"):
        return p["container_name"]
    rank = "0" if node_name == OPS["head"] else "1"
    return f"{OPS['container']}-r{rank}"


def _op_fleet_stop(jid, args):
    """Stop BOTH ranks (preserving). Head first: :8000 dies immediately, the
    worker then loses its TP peer and exits. Peers are both going down by
    definition, so the lone-rank guard does not apply."""
    force = bool(args.get("force"))
    results = {}
    for node in (OPS["head"], OPS["worker"]):
        rc, out, err = run_remote_sh(
            node,
            f"python3 {_launch_root()}/scripts/stop_preserving.py "
            f"--root {_launch_root()} --container "
            f"{shlex.quote(_rank_container(node))}",
            timeout=90)
        results[node] = {"rc": rc,
                         "note": (out.strip() or err.strip())[:120]}
        _job_note(jid, f"{node}: stop_preserving rc={rc} "
                  f"{(out.strip() or err.strip())[:80]}")
    log_event("fleet-stop", f"both ranks stopped (force={force}): {results}",
              job=jid)
    return {"ok": all(r["rc"] == 0 for r in results.values()),
            "ranks": results, "preserved": True}


def _op_fleet_start(jid, args):
    """Resume the PRESERVED pair: docker start worker (rank 1) then head
    (rank 0) - the launcher's documented restart path after a stop. Refuses
    unless both rank containers exist and are exited, and :8000 is silent."""
    probed = probe_node(OPS["head"])
    if not probed.get("reachable"):
        raise RuntimeError(f"head {OPS['head']} unreachable: "
                           f"{probed.get('error', 'probe failed')}")
    if probed.get("port8000"):
        # Fleet already live: a refusal, not a failure - report like the
        # launch/stop skip paths so the panel shows no error pill.
        _job_note(jid, ":8000 already answers - fleet live, nothing to start")
        return {"ok": True,
                "skipped": ":8000 already answers (fleet live); nothing to start"}
    for node in (OPS["worker"], OPS["head"]):
        p = probe_node(node)
        ranks = p.get("ranks") or {}
        rank = "0" if node == OPS["head"] else "1"
        d = ranks.get(rank)
        if not d:
            raise RuntimeError(f"{node}: no preserved container "
                               f"{OPS['container']}-r{rank}; use Fleet serve "
                               f"for a fresh deployment")
        if (d.get("status_raw") or "").startswith("Up"):
            _job_note(jid, f"{node}: {d['name']} already Up")
            continue
        _job_note(jid, f"docker start {d['name']} on {node}")
        rc, out, err = run_remote(node, f"docker start {shlex.quote(d['name'])}",
                                  timeout=60)
        if rc != 0:
            raise RuntimeError(f"docker start {d['name']} rc={rc}: "
                               f"{err.strip()[:200]}")
    for n in (OPS["worker"], OPS["head"]):
        LAST_LAUNCH[n] = time.time()
    log_event("fleet-start", "preserved ranks resumed (worker first)",
              job=jid)
    return {"ok": True, "op": "fleet-start"}


def _op_collect_logs(jid, args):
    node = args.get("node") or OPS["head"]
    tail = min(int(args.get("tail") or 600), 4000)
    rc, out, err = run_remote(
        node, f"docker logs --tail {tail} {shlex.quote(_rank_container(node))} 2>&1; "
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
        ctn = p.get("container_name") or f"{OPS['container']}-r?"
        lines.append(f"{role} {node} [{ctn}]: {phase}, disk={p.get('disk_free_g')}G, "
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
    tag = OPS.get("image_tag") or "glm53-roce:v11-b58f34ea"
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
    "fleet-stop": _op_fleet_stop,
    "fleet-start": _op_fleet_start,
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
            "variants": sorted(OPS["variants"]),
            "launcher": {"script": OPS.get("launch_script") or "",
                         "env_file": _launch_env_abs(),
                         "container": OPS.get("container") or "",
                         "image_tag": OPS.get("image_tag") or "",
                         "variant": _flash_weights(),
                         "variants": sorted(OPS["variants"])}}


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
        a = {"node": req.get("node"),
             "host": req.get("host"),
             "port": int(req.get("port") or 8000)}
        jid = job_submit("smoke", a,
                         f"smoke probe {a.get('host') or a.get('node') or 'head'}:{a['port']}")
        result = _op_smoke(jid, a)
        _job_set(jid, state="done", ended=_now(), result=result)
        return {"ok": True, "job": jid, "smoke": result}
    specs = {
        "launch": ("node", "variant"),
        "stop": ("node",),
        "fleet-stop": (),
        "fleet-start": (),
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
    desc = {"launch": f"fleet serve via launcher (variant={args.get('variant')})",
            "stop": f"stop rank {_rank_container(args.get('node'))} "
                    f"(preserving)",
            "fleet-stop": "fleet stop: both ranks (preserving)",
            "fleet-start": "fleet start: resume preserved ranks "
                           "(worker first)",
            "collect-logs": "collect engine logs",
            "audit": "fleet audit",
            "snapshot-image": "snapshot engine image"}[action]
    jid = job_submit(action, args, desc)
    return {"ok": True, "job": jid}
