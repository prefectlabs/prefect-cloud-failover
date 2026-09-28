# Keeping Time-Critical Flows Running Through Prefect Cloud Maintenance

**Goal:** Run a small self-hosted Prefect server inside your own network as a warm standby, so the handful of deployments that cannot wait keep starting on time while the Prefect Cloud API is unavailable, and everything reconciles back into Cloud afterwards.

**Who this is for:** Platform and data engineers already running Prefect Cloud with their own workers (hybrid work pools), with some flows that must start within minutes of their scheduled time.

**What to expect:** About half a day for the first setup. Most of that is standing up the server and a second worker. After that it runs itself. Every step links to the official docs. The only custom piece is a ~200-line watchdog flow, included in this repository and tested against Prefect 3.4.23.

Files in this repository:

| File | What it is |
|---|---|
| `README.md` | This guide |
| `dr_watchdog.py` | The failover watchdog flow (deploy on the standby server) |
| `prefect.yaml` | Deployment definition for the watchdog; points at this repository, so fork it or change the `pull` step |
| `requirements.txt` | What the watchdog needs at runtime (Prefect itself) |

---

## 1. What a maintenance window actually does to you

Prefect Cloud announces planned maintenance on [status.prefect.io](https://status.prefect.io) at least seven days ahead. Subscribe by email on the page, by RSS at `https://status.prefect.io/feed.rss`, or poll `https://status.prefect.io/api/v2/summary.json` (the `scheduled_maintenances` list). Windows are usually under ten minutes. The notice itself says a window may last up to an hour, and that work may run late but nothing is lost.

While the API is down, every request gets a retryable `503` with a `Prefect-Maintenance: true` header. Prefect clients from 3.3.1 onward ([PR #17667](https://github.com/PrefectHQ/prefect/pull/17667)) treat that header as "wait, don't give up" and keep retrying without counting attempts against `PREFECT_CLIENT_MAX_RETRIES`. In practice:

- **Workers** keep polling. They do not crash and do not need a restart.
- **Flow runs already executing** block at their next API call, then continue when the API returns. They are not lost.
- **Runs whose scheduled start falls inside the window cannot start.** They become `Late` and start as soon as the API is back.

So the one thing you lose is the ability to start new runs during the window. For daily and hourly batch work, "ten minutes late" is fine. For a 15-minute inference loop that feeds a trading desk or steers physical equipment, it is not. That gap is what the standby server closes.

## 2. The approach

```
          normal operation                          during a Cloud outage
  ┌─────────────────────────┐                ┌─────────────────────────┐
  │      Prefect Cloud       │   503 / down   │      Prefect Cloud       │
  │  all deployments active  │ ─────────────▶ │   (unreachable)          │
  └────────────┬────────────┘                └─────────────────────────┘
               │ polls                                    ▲ reconcile after:
   ┌───────────┴───────────┐                              │ cancel Late dupes,
   │  worker (Cloud)        │  your infra                  │ write audit artifact
   │  worker (standby)      │ ─────────┐                   │
   └───────────┬───────────┘          │  polls     ┌───────┴─────────────────┐
               │ nothing to do        └──────────▶ │  standby server (OSS)    │
   ┌───────────┴───────────┐                       │  "dr" deployments ACTIVE │
   │  standby server (OSS)  │                       │  everything else paused  │
   │  all schedules paused  │      watchdog every   └──────────────────────────┘
   │  watchdog: Cloud OK ✓  │      minute: 3 fails → resume "dr" deployments
   └───────────────────────┘
```

Four pieces:

1. **A standby Prefect server** (open source, self-hosted) runs permanently inside your network, next to your workers.
2. **It holds copies of your deployments**, all paused, so in normal operation it does nothing. Only deployments tagged `dr` are ever allowed to run there.
3. **A second worker** on your existing infrastructure polls the standby's work pool. Same pool name, same base job template, same flow code and images. Flows need no changes.
4. **A watchdog flow** on the standby runs every minute and probes Cloud. After three consecutive failures it resumes the `dr` deployments on the standby, so they start running there. When Cloud answers again it pauses them, cancels the duplicate `Late` runs that queued up in Cloud, and writes a markdown artifact into Cloud listing every run the standby executed.

Two design points worth knowing:

- **Detection has to live on your side.** Cloud automations run inside Cloud, so nothing in Cloud can react to Cloud being down. The watchdog runs on the one server that is still up.
- **Paused schedules, not a paused work pool.** A paused pool still lets the scheduler queue runs, and they all fire at once (stale) when you unpause. A paused deployment queues nothing; resuming it creates fresh runs from now.

## 3. Checklist

```
[ ] Prefect 3.4.14+ on the standby server, 3.3.1+ on workers and flow images   (prefect version)
[ ] A host or namespace for the standby: same network as your workers, reachable by them, not public
[ ] PostgreSQL for the standby (a small managed instance, or a container with a persistent volume). Not SQLite.
[ ] A basic-auth string for the standby server
[ ] A Cloud API key the watchdog can use (a service account with the Developer role is enough)
[ ] Values for any Secret / credential blocks the time-critical flows use (transfer does not copy secret values)
[ ] The list of deployments that are time-critical. Everything else stays Cloud-only.
```

## 4. Stand up the standby server

**Where it goes.** Same VPC or network as your workers, so the worker can reach it and it can reach nothing it shouldn't. If you already have a DR region, that is a fine home. Do not expose it to the internet. Sizing is modest: two vCPUs and 4 GB of RAM handle a few dozen deployments comfortably. Use PostgreSQL, not the default SQLite.

**Pick one:**

- **A VM with Docker Compose.** Follow [How to run the Prefect server via Docker Compose](https://docs.prefect.io/v3/how-to-guides/self-hosted/docker-compose). The reference `compose.yml` gives you Postgres, Redis, the API, background services, and a worker. Pin the image to the exact Prefect version your workers run (for example `prefecthq/prefect:3.4.23-python3.12`) instead of `3-latest`.
- **Kubernetes with Helm.** Follow [How to self-host the Prefect server with Helm](https://docs.prefect.io/v3/advanced/server-helm). Point it at your own Postgres and enable `server.basicAuth`.

**Lock it down.** Set `PREFECT_SERVER_API_AUTH_STRING="admin:<strong-password>"` on the server process, per [How to secure a self-hosted Prefect server](https://docs.prefect.io/v3/advanced/security-settings). Every client that talks to the standby then sets `PREFECT_API_AUTH_STRING` to the same value. Make sure `PREFECT_API_KEY` is **not** set on anything pointed at the standby, or you get `401`s.

**Verify:**

```bash
curl http://<standby-host>:4200/api/health     # -> true
```

Background reading: [Prefect server concepts](https://docs.prefect.io/v3/concepts/server), [How to scale self-hosted Prefect](https://docs.prefect.io/v3/advanced/self-hosted), [Database maintenance](https://docs.prefect.io/v3/advanced/database-maintenance).

## 5. Create two CLI profiles

Profiles let one laptop or CI runner talk to both environments. Details in [Settings and profiles](https://docs.prefect.io/v3/concepts/settings-and-profiles).

```bash
# Cloud (skip if you already have a Cloud profile; `prefect cloud login` also works)
prefect profile create cloud
prefect --profile cloud config set \
  PREFECT_API_URL=https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id> \
  PREFECT_API_KEY=<your-key>

# Standby
prefect profile create dr
prefect --profile dr config set \
  PREFECT_API_URL=http://<standby-host>:4200/api \
  PREFECT_API_AUTH_STRING=admin:<strong-password>

prefect --profile dr version      # should print "Server type: server"
```

The global `--profile` flag works on every command, so you never have to switch the active profile.

## 6. Copy your deployments to the standby

Prefect 3.4.14+ ships [`prefect transfer`](https://docs.prefect.io/v3/how-to-guides/migrate/transfer-resources), which copies resources between two profiles in dependency order:

```bash
prefect transfer --from cloud --to dr
```

It copies flows, deployments, work pools and queues, blocks, variables, global concurrency limits, and automations. It skips anything that already exists on the target, so it is safe to re-run, and it is a one-time seed rather than an ongoing sync. It does **not** copy run history, secret values inside blocks, or Cloud-only pool types (managed and push pools).

Immediately afterwards, pause everything on the standby. Nothing should ever run there unless the watchdog says so. (`--all` is safe at this point because the watchdog is not deployed yet; after that, use the watchdog's `standby` mode instead.)

```bash
prefect --profile dr --no-prompt deployment schedule pause --all
```

Then finish the seed by hand:

1. **Re-enter secret values** on the standby for any Secret or credentials blocks the time-critical flows use (UI, or `Secret(value=...).save("name")` with `PREFECT_PROFILE=dr`).
2. **Tag the time-critical deployments `dr`** in their deployment definition (`tags: [dr]` in `prefect.yaml`, or `tags=["dr"]` in `.deploy()`), then redeploy to both environments. The tag is what tells the watchdog which deployments it may run. Untagged deployments stay paused on the standby forever.

**Keep the two in sync from CI/CD.** Deployments drift, so add the standby to the same pipeline that deploys to Cloud. Your flow code and deployment definitions do not change; the pipeline gains two lines:

```bash
# Cloud: everything, exactly as today
prefect --profile cloud deploy --all

# Standby: only the time-critical deployments, then put the standby back to idle
prefect --profile dr deploy -n price-model/intraday -n wind-steering/15-minute
prefect --profile dr deployment run dr-watchdog/dr-watchdog --param mode=standby
```

Three rules for that pipeline:

- **Run the Cloud and standby deploys as independent steps** (separate jobs, or `continue-on-error`), so a Cloud outage during a deploy does not block the standby update, and vice versa.
- **Do not `deploy --all` to the standby.** Every deployment lands with its schedule active, and untagged ones would start running there. Name the time-critical ones with `-n` (patterns work: `-n 'critical-*/*'`).
- **Do not use `schedule pause --all` after the watchdog exists.** It pauses the watchdog too. `mode=standby` pauses only `dr`-tagged deployments, and does nothing if a failover is in progress at that moment.

The watchdog also re-pauses anything left active within a minute, but the explicit `standby` step avoids a stray duplicate run in that minute.

Deployments on both servers point at the same flow code (git repo, image, or object storage), so make sure the standby's worker can reach it. Since it is the same infrastructure, it usually already can.

## 7. Start a second worker against the standby

After the transfer, the standby has a work pool with the same name and base job template as Cloud. Start another worker for it, on the same infrastructure your Cloud worker uses. See [Workers](https://docs.prefect.io/v3/concepts/workers) and [How to manage work pools](https://docs.prefect.io/v3/how-to-guides/deployment_infra/manage-work-pools).

**Process or Docker worker on a VM:**

```bash
PREFECT_API_URL=http://<standby-host>:4200/api \
PREFECT_API_AUTH_STRING=admin:<strong-password> \
prefect worker start --pool <your-pool>
```

**Kubernetes:** install the `prefect-worker` chart a second time with `worker.apiConfig: selfHostedServer`, as shown in the [Helm guide](https://docs.prefect.io/v3/advanced/server-helm):

```yaml
worker:
  apiConfig: selfHostedServer
  config:
    workPool: <your-pool>
  selfHostedServerApiConfig:
    apiUrl: http://prefect-server.prefect.svc.cluster.local:4200/api
    basicAuth:
      enabled: true
      existingSecret: worker-auth-secret
```

The worker passes its own `PREFECT_API_URL` and `PREFECT_API_AUTH_STRING` into every flow run it launches, so runs report to the standby automatically. If your base job template hard-codes `PREFECT_API_URL` or `PREFECT_API_KEY` in its `env`, remove those on the standby's copy of the pool.

## 8. Deploy the watchdog

`dr_watchdog.py` in this folder is the whole failover mechanism. It runs on the standby server as a normal deployment on a one-minute schedule.

What it does each minute (`mode=auto`):

| Cloud probe | Standby state | Action |
|---|---|---|
| healthy | idle | Pause any `dr` deployment a deploy left active (hygiene) |
| unhealthy, 1st or 2nd time | idle | Count it, do nothing |
| unhealthy, 3rd consecutive | idle | **Failover:** resume all `dr` deployments on the standby |
| unhealthy | failed over | Stay failed over |
| healthy | failed over | **Restore:** pause `dr` deployments, cancel `dr`-tagged `Late` runs in Cloud, write a `dr-failover-report` artifact into Cloud |

The probe is an authenticated `GET /hello` against your Cloud workspace. A `401` or `403` means the credentials are wrong; the watchdog logs an error and does nothing rather than failing over on a misconfiguration.

Three more modes exist for humans and pipelines, passed as `--param mode=...`: `failover` (arm now), `restore` (stand down and reconcile), and `standby` (pause `dr` deployments unless a failover is active; this is the CI/CD step from section 6).

**Give it Cloud credentials.** Preferred: store them on the standby, so the key never sits in a deployment's job variables.

```bash
prefect --profile dr variable set cloud_api_url \
  https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>

PREFECT_PROFILE=dr python -c \
  "from prefect.blocks.system import Secret; Secret(value='<cloud-api-key>').save('cloud-api-key')"
```

(Alternatively set `CLOUD_API_URL` and `CLOUD_API_KEY` as environment variables on the job; see the comments in `prefect.yaml`.)

**Deploy it** to the standby only, from the included `prefect.yaml` after editing the `pull` step and pool name:

```bash
prefect --profile dr deploy -n dr-watchdog
```

Do not tag the watchdog `dr`. It would pause itself.

**Test it before you need it:**

```bash
# Arm: dr deployments resume on the standby right now
prefect --profile dr deployment run dr-watchdog/dr-watchdog --param mode=failover
# ...check the standby UI: dr deployments show upcoming runs; the rest do not.

# Restore: pauses them, reconciles with Cloud, writes the artifact
prefect --profile dr deployment run dr-watchdog/dr-watchdog --param mode=restore
# ...check Cloud UI > Artifacts for "dr-failover-report".
```

## 9. Runbook for an announced window

**Hands-off (default).** Do nothing. The watchdog trips about three minutes after the API goes dark (three probes, one minute apart) and restores itself on the first healthy probe afterwards. Your exposure is the trip time plus the interval of the deployment: for a 15-minute schedule, at most one delayed run.

**Zero gap.** If even that is too much, arm the standby ten minutes before the window:

```bash
prefect --profile dr deployment run dr-watchdog/dr-watchdog --param mode=failover
```

An armed standby stays active through the window and restores itself automatically once Cloud has gone down and come back. Until Cloud actually goes down, the `dr` deployments run in **both** places, so only pre-arm flows whose writes are safe to repeat (for example "compute and overwrite the latest prediction"). To stand down early, run `mode=restore`.

**After the window.** Open Cloud, go to Artifacts, and read `dr-failover-report`. It lists every run the standby executed, with state and timings, and which `Late` runs in Cloud were cancelled because the standby had covered them. That is your audit trail; the standby's own UI has the logs.

## 10. Tips and gotchas

- **Raise client retries for unplanned outages.** The maintenance header only covers planned windows. For anything else that returns `502`/`503` without it, a flow run gives up after `PREFECT_CLIENT_MAX_RETRIES` attempts with exponential backoff: the default of 5 survives roughly two minutes, 8 roughly 17 minutes, 10 roughly an hour. Set `PREFECT_CLIENT_MAX_RETRIES=10` in your workers' environment and in the work pool's job `env`. Reference: [settings](https://docs.prefect.io/v3/api-ref/settings-ref).
- **Make time-critical flows idempotent.** During failover, a run can execute on the standby while its Cloud twin sits `Late`. The watchdog cancels those twins on restore, but a run that started in the seconds between Cloud returning and the next watchdog tick will execute. Writes that overwrite by key, or check a freshness timestamp, make that harmless.
- **The cancel step is tag-based.** On restore, every `Late` Cloud run carrying the `dr` tag is cancelled, including any that were `Late` for an unrelated reason (a stopped worker, say). Set `cancel_cloud_late_runs_on_restore: false` on the watchdog deployment if you would rather let them run.
- **Version discipline.** Keep the standby server version at or above your worker and flow-image versions. Upgrade the standby when you upgrade the workers; it is part of production now.
- **Monitor the standby.** Alert on `GET /api/health` and on the watchdog's own deployment going stale (no run in five minutes). A standby nobody watches is a standby that is down when you need it.
- **Rehearse quarterly.** Run `failover` then `restore` outside a window and confirm runs appear on the standby and the report appears in Cloud. Five minutes, and you know it works.
- **Keep the standby small on purpose.** Only `dr`-tagged deployments ever run there. Long-running training jobs, backfills, and reports can wait for Cloud to return; leave them untagged.
- **Concurrency limits and automations transferred too.** Global concurrency limits carry over with the same names, so a failover respects the same limits. Notification automations also transfer, but the blocks behind them (Slack webhooks, email credentials) need their secret values re-entered.

## 11. Use the Prefect MCP server while you build

Prefect ships a read-only [MCP server](https://docs.prefect.io/v3/how-to-guides/ai/use-prefect-mcp-server) that gives Claude Code, Cursor, Codex, and similar assistants two things that help here: Prefect documentation search, and a live read-only view of any Prefect API, Cloud or self-hosted. Register it twice, once per environment, and you can ask your assistant to compare them.

```bash
# Cloud
claude mcp add prefect-cloud \
  -e PREFECT_API_URL=https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id> \
  -e PREFECT_API_KEY=<your-key> \
  -- uvx --from prefect-mcp prefect-mcp-server

# Standby
claude mcp add prefect-standby \
  -e PREFECT_API_URL=http://<standby-host>:4200/api \
  -e PREFECT_API_AUTH_STRING=admin:<strong-password> \
  -- uvx --from prefect-mcp prefect-mcp-server
```

Cursor and Codex take the same command, arguments, and environment in their own config files; the docs page has the snippets. It cannot change anything, so engineers can use it freely. Questions worth asking it during the build:

- "List the deployments tagged `dr` on prefect-standby and confirm every schedule is paused."
- "Compare deployments on prefect-cloud and prefect-standby by name and tag. What is missing on the standby?"
- "Show the last 20 events on prefect-standby." (after a rehearsal, to see the watchdog's resume and pause)
- "Search the Prefect docs for what `prefect transfer` does not copy."

## 12. Links

| Topic | Link |
|---|---|
| Status page, RSS, JSON | https://status.prefect.io · https://status.prefect.io/feed.rss · https://status.prefect.io/api/v2/summary.json |
| Maintenance-aware client retries | https://github.com/PrefectHQ/prefect/pull/17667 |
| Self-hosted server concepts | https://docs.prefect.io/v3/concepts/server |
| Docker Compose setup | https://docs.prefect.io/v3/how-to-guides/self-hosted/docker-compose |
| Helm setup (server + worker) | https://docs.prefect.io/v3/advanced/server-helm · https://github.com/PrefectHQ/prefect-helm |
| Securing the server (basic auth) | https://docs.prefect.io/v3/advanced/security-settings |
| Scaling and database maintenance | https://docs.prefect.io/v3/advanced/self-hosted · https://docs.prefect.io/v3/advanced/database-maintenance |
| Settings and profiles | https://docs.prefect.io/v3/concepts/settings-and-profiles |
| Connect to Cloud, API keys, service accounts | https://docs.prefect.io/v3/how-to-guides/cloud/connect-to-cloud · https://docs.prefect.io/v3/how-to-guides/cloud/manage-users/service-accounts |
| `prefect transfer` | https://docs.prefect.io/v3/how-to-guides/migrate/transfer-resources |
| Pause and resume schedules | https://docs.prefect.io/v3/how-to-guides/deployments/manage-schedules |
| Work pools and workers | https://docs.prefect.io/v3/how-to-guides/deployment_infra/manage-work-pools · https://docs.prefect.io/v3/concepts/workers |
| `prefect.yaml` and job variables | https://docs.prefect.io/v3/how-to-guides/deployments/prefect-yaml · https://docs.prefect.io/v3/how-to-guides/deployments/customize-job-variables |
| Client settings (`max_retries`) | https://docs.prefect.io/v3/api-ref/settings-ref |
| Prefect MCP server | https://docs.prefect.io/v3/how-to-guides/ai/use-prefect-mcp-server · https://github.com/PrefectHQ/prefect-mcp-server |
