import json, os
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException
from src.utils import repository as repo
from src.utils.identity import new_id, now
from src.services.statistics import analyze
from src.config.settings import TASK_MODE, REDIS_URL, ARTIFACT_DIR, RUN_LEASE_SECONDS
from src.constants.domain import MAX_QUEUED

QUEUE_KEY = "stratum-hidden:jobs"
RECOVERY_KEY = "stratum-hidden:recovery:"

def _at(value):
    if not value: return None
    try: return datetime.fromisoformat(value)
    except (TypeError, ValueError): return None

def _lease_expired(run):
    """A queued/running activation is stale once its lease has passed."""
    if run.get("status") not in ("queued", "running"): return False
    lease = _at(run.get("lease_until"))
    return lease is None or lease <= datetime.now(timezone.utc)

def _next_lease():
    return (datetime.now(timezone.utc) + timedelta(seconds=RUN_LEASE_SECONDS)).isoformat()

def _latest(runs):
    return max(runs, key=lambda r: (r.get("created_at", ""), r.get("id", ""))) if runs else None

def _view(run, study):
    """UI projection: recovery/fencing state, without the internal executor token."""
    item = {k: v for k, v in run.items() if k not in ("executor", "recovery_pushed_at")}
    item["attempt"] = run.get("attempt", 0)
    item["recoveries"] = run.get("recoveries", 0)
    item["recoverable"] = _lease_expired(run)
    item["retryable"] = item["recoverable"] or run.get("status") == "failed"
    if study:
        snapshot = study.get("snapshot", {})
        model = snapshot.get("model", {})
        item.update(study_name=study.get("name", ""), borehole=snapshot.get("borehole", ""),
                    checksum=study.get("checksum", ""), model_id=model.get("id", ""),
                    model_name=model.get("name", ""), model_revision=model.get("revision"))
    return item

def list_runs():
    studies = {s["id"]: s for s in repo.all_rows("studies")}
    return [_view(r, studies.get(r.get("study_id"))) for r in sorted(repo.all_rows("runs"), key=lambda r: (r.get("created_at", ""), r.get("id", "")))]

def detail(id):
    run = repo.get("runs", id)
    return _view(run, repo.get("studies", run["study_id"]) if run.get("study_id") else None)

def _arm_resume(run, interrupted):
    """Reset an activation to ``queued`` in place so the next claim re-executes it.

    ``study_id`` is never touched: the original borehole and model snapshot are
    reused, and the run keeps its identity for reviews and reports. The
    ``interrupted_pending`` flag lets the eventual claim count the recovery
    exactly once, even when several retries race.
    """
    if interrupted:
        run["last_interrupted_at"] = now()
        run["interrupted_pending"] = True
    run.update(status="queued", executor="", lease_until=_next_lease(), error="", recovery_pushed_at="")

def _claim(run_id, token):
    """Atomically take ownership; exactly one executor wins duplicate deliveries.

    A ``running`` run can only be re-claimed after its lease expired, which is
    what makes interrupted execution resumable. ``attempt`` fences stale writers.
    """
    with repo.transaction() as (conn, q):
        run = repo.read(conn, "runs", run_id, q, lock=True, missing_ok=True)
        if run is None: return None
        status = run.get("status")
        if status not in ("queued", "running"): return None
        if status == "running" and not _lease_expired(run): return None
        if status == "running" or run.get("interrupted_pending"):
            run["recoveries"] = run.get("recoveries", 0) + 1
            run["last_interrupted_at"] = now()
        run.update(attempt=run.get("attempt", 0) + 1, status="running", executor=token,
                   lease_until=_next_lease(), started_at=now(), error="",
                   recovery_pushed_at="", interrupted_pending=False)
        repo.upsert(conn, "runs", run, q)
        return run

def _finish(run_id, token, attempt, result=None, error=""):
    """Commit a result only for the activation that still owns the run.

    A superseded executor (older token or attempt), a late duplicate arriving
    after completion, or a run reset by retry is ignored. The first valid result
    is authoritative; recovery can never be overwritten by a stale executor.
    """
    with repo.transaction() as (conn, q):
        run = repo.read(conn, "runs", run_id, q, lock=True, missing_ok=True)
        if run is None: return None, False
        if run.get("status") == "completed": return run, False
        if run.get("status") != "running" or run.get("executor") != token or run.get("attempt") != attempt:
            return run, False
        run.update(executor="", finished_at=now(), lease_until=now())
        if result is None:
            run.update(status="failed", error=error, failed_attempt=attempt)
        else:
            run.update(status="completed", result=result, error="", result_attempt=attempt)
        repo.upsert(conn, "runs", run, q)
        return run, True

def _write_artifact(run_id, result):
    target = ARTIFACT_DIR / (run_id + ".json")
    try:
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(".json.tmp")
        temp.write_text(json.dumps(result, ensure_ascii=False))
        os.replace(temp, target)
    except OSError:
        pass

def execute(id):
    token = new_id("exec")
    claimed = _claim(id, token)
    if claimed is None:
        # Duplicate delivery, still-healthy lease, or already final: leave as is.
        return detail(id)
    attempt = claimed["attempt"]
    run = claimed
    try:
        study = repo.get("studies", claimed["study_id"])
        result = analyze(study["snapshot"], study["checksum"])
        run, accepted = _finish(id, token, attempt, result=result)
        if accepted: _write_artifact(id, result)
    except Exception as exc:
        run, _ = _finish(id, token, attempt, error=str(exc))
    return _view(run, None) if run else detail(id)

def _dispatch(run_id):
    if TASK_MODE == "inline": return execute(run_id)
    try:
        import redis
        redis.Redis.from_url(REDIS_URL).rpush(QUEUE_KEY, run_id)
    except Exception:
        message = "队列不可用，请恢复Redis后重试；原运行、孔段与模型快照均保留"
        with repo.transaction() as (conn, q):
            run = repo.read(conn, "runs", run_id, q, lock=True, missing_ok=True)
            if run is not None and run.get("status") == "queued":
                run.update(lease_until=now(), error=message)   # immediately recoverable
                repo.upsert(conn, "runs", run, q)
        raise ValueError(message)

def create(data):
    study_id = data["study_id"] if isinstance(data, dict) else data.study_id
    repo.get("studies", study_id)
    dispatch_id, run_id = None, None
    with repo.transaction() as (conn, q):
        # Locking the study serialises concurrent create/retry for one study,
        # so repeated initiation reuses the run instead of creating another.
        repo.read(conn, "studies", study_id, q, lock=True)
        runs = [r for r in repo.rows_in(conn, "runs", q) if r.get("study_id") == study_id]
        latest = _latest(runs)
        if latest is None:
            active = sum(r.get("status") in ("queued", "running") for r in repo.rows_in(conn, "runs", q))
            if active >= MAX_QUEUED: raise ValueError("运行队列已满，请等待已有任务完成")
            run = {"id": new_id("run"), "study_id": study_id, "status": "queued", "result": {}, "error": "", "created_at": now()}
            run.update(attempt=0, executor="", recoveries=0, lease_until=_next_lease(), recovery_pushed_at="")
            repo.upsert(conn, "runs", run, q)
            dispatch_id, run_id = run["id"], run["id"]
        elif latest.get("status") in ("queued", "running"):
            run_id = latest["id"]
            # Re-read under the run lock: a concurrent retry/worker may have
            # changed it since the list snapshot above.
            fresh = repo.read(conn, "runs", run_id, q, lock=True)
            if fresh.get("status") in ("queued", "running") and _lease_expired(fresh):
                _arm_resume(fresh, interrupted=True)
                repo.upsert(conn, "runs", fresh, q)
                dispatch_id = run_id
        else:
            # Completed or failed (possibly under review): keep it untouched.
            run_id = latest["id"]
    if dispatch_id: _dispatch(dispatch_id)
    return detail(run_id)

def retry(id):
    dispatch_id = None
    with repo.transaction() as (conn, q):
        run = repo.read(conn, "runs", id, q, lock=True, missing_ok=True)
        if run is None: raise HTTPException(404, "记录不存在")
        status = run.get("status")
        reviews = repo.rows_in(conn, "reviews", q)
        if any(r.get("run_id") == id and r.get("status") in ("pending", "approved") for r in reviews):
            raise HTTPException(409, "运行已提审，结果保持不变；请先完成或退回复核")
        if status == "completed":
            raise HTTPException(409, "运行已完成，结果保持不变；请复核或生成报告")
        if status in ("queued", "running"):
            if not _lease_expired(run):
                return _view(run, None)   # an activation is already in flight
            _arm_resume(run, interrupted=True)
            dispatch_id = id
        elif status == "failed":
            _arm_resume(run, interrupted=False)
            dispatch_id = id
        else:
            raise HTTPException(409, "当前运行不可恢复")
        repo.upsert(conn, "runs", run, q)
    if dispatch_id: _dispatch(dispatch_id)
    return detail(id)

def recover_stale():
    """Re-enqueue interrupted runs whose lease expired; safe to call repeatedly.

    The dedupe key keeps recovery from piling duplicate deliveries; even if it
    slips through, the claim in ``execute`` still accepts only one executor.
    """
    recovered = []
    try:
        import redis
        client = redis.Redis.from_url(REDIS_URL)
        for run in repo.all_rows("runs"):
            if not _lease_expired(run): continue
            # Claim the scan slot first; a concurrent user retry may re-arm the
            # run while we are deciding, which the row lock re-check catches.
            if not client.set(RECOVERY_KEY + run["id"], "1", nx=True, ex=max(30, RUN_LEASE_SECONDS)):
                continue
            with repo.transaction() as (conn, q):
                current = repo.read(conn, "runs", run["id"], q, lock=True, missing_ok=True)
                if current is None or not _lease_expired(current): continue
                _arm_resume(current, interrupted=True)
                repo.upsert(conn, "runs", current, q)
            client.rpush(QUEUE_KEY, run["id"])
            recovered.append(run["id"])
    except Exception:
        pass
    return recovered
