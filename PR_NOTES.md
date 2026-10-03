# Fleet Ops PR — engineering notes for review

## What ships
`fleet_ops.py` (std-lib only) + three integration points in `server.py`
(import, 2 hook lines in `main()`, `do_POST` + 3 `do_GET` branches), one CSS
+ HTML + JS block appended to the single-page UI (`OPS PANEL` section).

## Backend contract
- `POST /api/ops/<action>` — JSON body `{key, ...}`:
  - `smoke {host, port}` — inline curl of /v1/models, journaled like a job
  - `launch {node, variant}` — preflight-gated `launch-glm53.sh <rank> <variant>`
  - `stop {node[, force]}` — guarded docker rm -f
  - `collect-logs` — docker logs --tail 600 → data/bootlogs/
  - `audit` — phases + drift + upstream sha, read-only
  - `snapshot-image` — `docker save | gzip -1` streamed to ghost backup dir
- `GET /api/ops/state|/api/ops/job/<id>|/api/ops/events` — read-only, no token
- Keys: config.json `server.ops_key` or `SPARKY_OPS_KEY` env (default `sparky-ops-local`)
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

## Bootstrap
The panel notes `API-KEY: sparky-ops-local (default)` until the operator writes a real key into
config.json `server.ops_key`. Local-tailnet threat model; documented, not hidden.

## Verification performed (2026-10-03)
- pyflakes clean both files; 5/5 existing unit tests pass.
- Hermetic end-to-end: fake `_run` drive of probe parse, phase classification
  (`Up`+port ⇒ serving), king audit job (writes event, baseline, upstream check), job error
  paths, token gating, launch purge guard. 8/8 assertions green.
- Live soak: service restarted on ghost (`systemctl --user restart`), 8-minute soak with
  read-only probes hitting both real nodes every 10s over the tailnet — zero container
  restarts on either Spark (verified `docker inspect` orchards before/after),
  Hermès endpoint 200 throughout.
- UI: panel renders, state/actions/audit verified over the live HTTP API.

## Known limitations
- Single mutation worker (by design: one heavyweight ops transition at a time).
- `api_job` history is in-memory; restart clears job list (events log persists to disk).
- Image snapshot hard-codes the v11-dflash2 tag; move to config when a second image appears.
