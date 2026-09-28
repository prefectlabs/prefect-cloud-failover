"""
dr_watchdog.py - Prefect Cloud maintenance failover watchdog

Deploy this flow on your SELF-HOSTED (standby) Prefect server on a one-minute schedule.
Every run it probes Prefect Cloud and:

  * on sustained failure -> resumes the DR-tagged deployments on this server, so
                            time-critical work keeps starting on time
  * on recovery          -> pauses them again, cancels the duplicate "Late" runs that
                            queued up in Cloud while it was down (optional), and writes
                            a markdown artifact into Cloud listing everything the standby
                            ran, so the audit trail lives in one place

Modes (flow parameter `mode`):
  auto      probe Cloud and decide            (default; what the schedule uses)
  failover  arm the standby now: DR deployments run here until Cloud has gone down and
            come back, or until you run `restore`. While Cloud is still up, the same
            deployments run in BOTH places, so only pre-arm flows that tolerate that.
  restore   pause DR deployments here and reconcile with Cloud (undoes `failover`)
  standby   pause DR deployments here unless a failover is active; touches nothing else.
            Run this from CI/CD right after deploying to the standby, because `prefect deploy`
            re-activates schedules on its target.

Configuration (environment variables on the standby worker/job, or stored on the standby server):
  CLOUD_API_URL   https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>
                  (fallback: Prefect Variable `cloud_api_url` on the standby server)
  CLOUD_API_KEY   Cloud API key (service account) that can read/cancel flow runs and create artifacts
                  (fallback: Secret block `cloud-api-key` on the standby server, so the key never sits in job variables)
  DR_TAG          tag that marks deployments the standby may run (default: "dr")
  DR_TRIP_AFTER   consecutive failed probes before failing over (default: 3)

Do NOT tag this flow's own deployment with DR_TAG, or it will pause itself.
Tested with Prefect 3.4.23.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Literal

import httpx
from prefect import flow, get_run_logger
from prefect.blocks.system import Secret
from prefect.client.orchestration import PrefectClient, get_client
from prefect.client.schemas.actions import ArtifactCreate
from prefect.client.schemas.filters import (
    DeploymentFilter,
    DeploymentFilterTags,
    FlowRunFilter,
    FlowRunFilterStartTime,
    FlowRunFilterState,
    FlowRunFilterStateName,
    FlowRunFilterTags,
)
from prefect.states import Cancelled
from prefect.variables import Variable

DR_TAG = os.environ.get("DR_TAG", "dr")
TRIP_AFTER = int(os.environ.get("DR_TRIP_AFTER", "3"))
STATE_VAR = "dr_watchdog_state"  # Prefect Variable on the standby server that carries state between runs


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def cloud_credentials() -> tuple[str, str]:
    """Cloud URL/key from the environment, else from a Variable + Secret block stored on the standby server."""
    url = os.environ.get("CLOUD_API_URL") or await Variable.aget("cloud_api_url")
    key = os.environ.get("CLOUD_API_KEY")
    if not key:
        try:
            key = (await Secret.aload("cloud-api-key")).get()
        except ValueError:
            key = None
    if not url or not key:
        raise RuntimeError(
            "Cloud credentials missing: set CLOUD_API_URL and CLOUD_API_KEY, "
            "or create Variable `cloud_api_url` and Secret block `cloud-api-key` on the standby server"
        )
    return str(url).rstrip("/"), str(key)


def _cloud(url: str, key: str) -> PrefectClient:
    return PrefectClient(api=url, api_key=key)


async def cloud_is_healthy(url: str, key: str, logger) -> bool | None:
    """True = healthy, False = down, None = probe misconfigured (never fail over on None)."""
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            r = await http.get(url + "/hello", headers={"Authorization": f"Bearer {key}"})
    except httpx.HTTPError as exc:
        logger.warning(f"Cloud probe failed: {exc!r}")
        return False
    if r.status_code == 200:
        return True
    if r.status_code in (401, 403):
        logger.error(f"Cloud probe returned {r.status_code}. Check CLOUD_API_URL / CLOUD_API_KEY. Doing nothing.")
        return None
    logger.warning(
        f"Cloud probe returned {r.status_code} "
        f"(Prefect-Maintenance header: {r.headers.get('Prefect-Maintenance', 'absent')})"
    )
    return False


async def set_dr_deployments(dr: PrefectClient, active: bool, logger) -> int:
    """Resume (active=True) or pause (active=False) every DR-tagged deployment on this server.

    Prefect has two independent switches the scheduler honours: the deployment's own
    `paused` flag and each schedule's `active` flag. Flip both, so it works no matter how
    the deployment was created. Pausing also deletes the runs the standby scheduler had queued.
    """
    deployments = await dr.read_deployments(
        deployment_filter=DeploymentFilter(tags=DeploymentFilterTags(all_=[DR_TAG]))
    )
    changed = 0
    for d in deployments:
        touched = False
        for s in d.schedules:
            if s.active != active:
                await dr.update_deployment_schedule(d.id, s.id, active=active)
                touched = True
        if d.paused == active:  # paused while activating, or unpaused while pausing
            if active:
                await dr.resume_deployment(d.id)
            else:
                await dr.pause_deployment(d.id)
            touched = True
        if touched:
            changed += 1
            logger.info(f"{'Resumed' if active else 'Paused'} {d.name}")
    return changed


async def cancel_cloud_late_runs(cloud: PrefectClient, logger) -> list[str]:
    """Cancel DR-tagged runs that went Late in Cloud during the outage (the standby covered them)."""
    late = await cloud.read_flow_runs(
        flow_run_filter=FlowRunFilter(
            tags=FlowRunFilterTags(all_=[DR_TAG]),
            state=FlowRunFilterState(name=FlowRunFilterStateName(any_=["Late"])),
        )
    )
    cancelled: list[str] = []
    for run in late:
        result = await cloud.set_flow_run_state(
            run.id, Cancelled(message="Covered by the standby server during Prefect Cloud maintenance")
        )
        logger.info(f"Cancelled Late Cloud run {run.name}: {result.status}")
        cancelled.append(run.name)
    return cancelled


async def write_audit_artifact(cloud: PrefectClient, dr: PrefectClient, activated_at: str, cancelled: list[str]) -> None:
    """Record in Cloud what the standby ran while Cloud was unavailable."""
    runs = await dr.read_flow_runs(
        flow_run_filter=FlowRunFilter(
            tags=FlowRunFilterTags(all_=[DR_TAG]),
            start_time=FlowRunFilterStartTime(after_=datetime.fromisoformat(activated_at)),
        )
    )
    rows = "\n".join(
        f"| {r.name} | {r.state_name} | {r.start_time} | {r.end_time} | {r.id} |" for r in runs
    ) or "| (none) | | | | |"
    body = (
        "## DR failover report\n\n"
        f"- Failover activated: {activated_at}\n"
        f"- Restored: {_now().isoformat()}\n"
        f"- Cloud Late runs cancelled: {len(cancelled)} {cancelled}\n\n"
        "### Runs executed on the standby server\n\n"
        "| Run | State | Start | End | Standby run id |\n|---|---|---|---|---|\n" + rows
    )
    await cloud.create_artifact(
        ArtifactCreate(
            key="dr-failover-report",
            type="markdown",
            description="What the self-hosted standby server ran while Prefect Cloud was unavailable",
            data=body,
        )
    )


@flow(name="dr-watchdog", log_prints=True)
async def dr_watchdog(
    mode: Literal["auto", "failover", "restore", "standby"] = "auto",
    cancel_cloud_late_runs_on_restore: bool = True,
) -> dict:
    logger = get_run_logger()
    cloud_url, cloud_key = await cloud_credentials()

    state = await Variable.aget(STATE_VAR, default={}) or {}
    failures = int(state.get("failures", 0))
    active = bool(state.get("active", False))
    activated_at = state.get("activated_at")
    armed = bool(state.get("armed", False))            # failover was requested manually
    saw_outage = bool(state.get("saw_outage", False))  # Cloud has been down since arming

    async with get_client() as dr:  # this flow runs on the standby server, so get_client() IS the standby

        async def failover(manual: bool) -> None:
            nonlocal active, activated_at, armed, saw_outage
            n = await set_dr_deployments(dr, True, logger)
            active, activated_at, armed, saw_outage = True, _now().isoformat(), manual, False
            logger.warning(f"FAILOVER ACTIVE ({'armed manually' if manual else 'automatic'}): resumed {n} DR deployment(s)")

        async def restore() -> None:
            nonlocal active, activated_at, failures, armed, saw_outage
            n = await set_dr_deployments(dr, False, logger)
            logger.warning(f"RESTORE: paused {n} DR deployment(s) on this server")
            cancelled: list[str] = []
            try:
                async with _cloud(cloud_url, cloud_key) as cloud:
                    if cancel_cloud_late_runs_on_restore:
                        cancelled = await cancel_cloud_late_runs(cloud, logger)
                    await write_audit_artifact(cloud, dr, activated_at or _now().isoformat(), cancelled)
            except Exception as exc:  # Cloud reconciliation must never block returning the standby to idle
                logger.error(f"Cloud reconciliation failed (DR deployments are paused regardless): {exc!r}")
            active, activated_at, failures, armed, saw_outage = False, None, 0, False, False

        if mode == "failover":
            if not active:
                await failover(manual=True)
            else:
                logger.info("Failover already active")
        elif mode == "restore":
            if active:
                await restore()
            else:
                await set_dr_deployments(dr, False, logger)
        elif mode == "standby":
            if active:
                logger.info("Failover is active; leaving DR deployments running")
            else:
                n = await set_dr_deployments(dr, False, logger)
                logger.info(f"Standby: paused {n} DR deployment(s)")
        else:  # auto
            healthy = await cloud_is_healthy(cloud_url, cloud_key, logger)
            if healthy is None:
                return {"skipped": "probe misconfigured"}
            if not healthy:
                failures += 1
                saw_outage = saw_outage or active
                logger.warning(f"Cloud unhealthy: {failures}/{TRIP_AFTER} consecutive failed probes")
                if failures >= TRIP_AFTER and not active:
                    await failover(manual=False)
            else:
                failures = 0
                if active and armed and not saw_outage:
                    logger.info("Armed for an announced window; Cloud still healthy, standing by (running in both places)")
                elif active:
                    await restore()
                else:
                    # Standby hygiene: anything a CI deploy left active gets paused again.
                    n = await set_dr_deployments(dr, False, logger)
                    if n:
                        logger.info(f"Standby: paused {n} DR deployment(s) that were left active")

    new_state = {
        "failures": failures,
        "active": active,
        "activated_at": activated_at,
        "armed": armed,
        "saw_outage": saw_outage,
        "checked_at": _now().isoformat(),
    }
    await Variable.aset(STATE_VAR, new_state, overwrite=True)
    logger.info(f"State: {new_state}")
    return new_state


if __name__ == "__main__":
    import asyncio
    import sys

    # Manual use from a laptop with the standby profile active:  prefect --profile dr ... or PREFECT_PROFILE=dr python dr_watchdog.py failover
    asyncio.run(dr_watchdog(mode=sys.argv[1] if len(sys.argv) > 1 else "auto"))  # type: ignore[arg-type]
