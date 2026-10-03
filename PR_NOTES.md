# Fleet Ops — engineering notes

Status: LIVE on ghost since 2026-10-03 (main @ d6d0dba + 82da3a6, 0b402ad; tag
tier3-v1). Offsite mirror: fork GlitchHawk/The-Sparky-Command-Center (main +
tier3). Personal deployment of tonyd2wild's dashboard — not upstream-bound;
`origin` stays read-only for audit drift comparison.

## What ships
`fleet_ops.py` (std-lib only) + three integration points in `server.py`
(import, 2 hook lines in `main()`, `do_POST` + 3 `do_GET` branches), one CSS
+ HTML + JS block appended to the single-page UI (`OPS PANEL` section).

## Backend contract
- `POST /api/ops/<action>` — key accepted in query string OR JSON body; body
  wins on collision:
  - `smoke {node|host, port}` — engine probe; `node` resolves via config
    (head spark serves :8000; worker spark2 never binds it — selecting it
    returns guidance, not a silent failure)
  - `launch {node, variant}` — preflight-gated `launch-glm53.sh <rank> <variant>`
  - `stop {node[, force]}` — guarded docker rm -f
  - `collect-logs` — docker logs --tail 600 → data/bootlogs/
  - `audit` — phases + drift + upstream sha, read-only
  - `snapshot-image` — `docker save | gzip -1` streamed to ghost backup dir
- `GET /api/ops/state|/api/ops/job/<id>|/api/ops/events` — read-only, no token
- Key config: `server.ops_key` in config.json or `SPARKY_OPS_KEY` env
  (default `sparky-ops-local`)
- Background daemon threads: ops-probe (10s per node, read-only ssh poll) and
  ops-jobs (serialized mutation queue). Both die with the process; no timers, no systemd units, no cron.

## Safety properties (why the cluster cannot be disturbed by the buildout)
1. Launches refuse when a container is already `Up` on that node (no double-rank risk, no clobber).
2. Preflight gate: MemAvailable ≥ 8G, /var/tmp free ≥ 40G, patches + drafter present (crash-lesson thresholds).
3. Stop (no `force`) refuses while the peer rank serves — no lone-rank kill resets.
4. `restart=no` containers are untouched at deploy; the panel mutation surface on nodes is exactly
   {launch-glm53.sh, docker rm -f, docker logs, docker ps/images/rm, ss, df, free, stat} —
   the same verbs a human types, nothing installed, no daemon edits.
5. Ghost: no reboot in scope; service restart is user-level, systemd `Restart=on-failure` covers it.
   Hermes harness and all other ghost services are untouched.

## Field-tested quirks
- Worker rank never binds :8000 and containers carry no docker healthcheck, so
  worker "serving" = Up + no dashboard launch in the last 35 min
  (WORKER_BOOT_WINDOW_S); the head must answer on :8000.
- MemAvailable on the Sparks reads 0.8-1.2G while serving (121/122G used,
  112G cgroup cap) — normal for this stack, not a warning.
- The panel stores the ops key in localStorage, prompted once.

## Verification performed (2026-10-03)
- pyflakes clean; 5/5 pre-existing unit tests pass; full page JS node --check OK.
- Hermetic battery: probe parse, phase-classification truth table, job lifecycle
  incl. error paths, key gating, purge guard.
- Live: both real ranks classified `serving`; smoke via API returned
  `glm-5.3-flash`; audit job completed (drift + upstream sha); UI-exact click
  shapes replayed via curl after two real UI bugs were fixed (query-key auth;
  Smoke default target).
- Cluster proof: containers `started=2026-09-20…, restarts=0` before and after
  the entire buildout.

## Known limitations
- Single mutation worker (by design: one heavyweight ops transition at a time).
- `api_job` history is in-memory; restart clears job list (events log persists to disk).
- Image snapshot hard-codes the v11-dflash2 tag; move to config when a second image appears.
