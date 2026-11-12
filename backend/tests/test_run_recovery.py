"""Focused tests for interrupted-run recovery, fencing and archive consistency.

Run from /workspace/backend with:  python3 -m unittest discover -s tests
Uses SQLite + inline execution, isolated in a temp database per test.
"""
import os, tempfile, threading, unittest
from datetime import datetime, timedelta, timezone

os.environ.setdefault("TASK_MODE", "inline")
os.environ.setdefault("SEED_DEMO", "false")
os.environ.setdefault("RUN_LEASE_SECONDS", "60")


def _reload_modules(db_path):
    os.environ["DATABASE_URL"] = "sqlite:///" + db_path
    os.environ["TASK_MODE"] = "inline"
    os.environ["ARTIFACT_DIR"] = tempfile.mkdtemp(prefix="artifacts-")
    import importlib, src.config.settings as settings
    importlib.reload(settings)
    import src.utils.repository as repository
    importlib.reload(repository)
    import src.services.statistics as statistics
    importlib.reload(statistics)
    import src.services.run as run_service
    importlib.reload(run_service)
    import src.services.review as review_service
    importlib.reload(review_service)
    import src.services.report as report_service
    importlib.reload(report_service)
    import src.services.seed as seed_service
    importlib.reload(seed_service)
    return run_service, review_service, report_service, seed_service, settings


class RunRecoveryTests(unittest.TestCase):
    def setUp(self):
        fd, self.db = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        os.unlink(self.db)
        self.run, self.review, self.report, self.seed, self.settings = _reload_modules(self.db)
        self.seed.seed()
        # The seeded study already owns an approved review, so recovery tests
        # use their own locked study with the same borehole/model snapshot.
        base = self.run.repo.get("studies", "study-sequence")
        self.study_id = "study-test"
        self.run.repo.put("studies", {**base, "id": self.study_id, "name": "测试孔段研究"})

    def tearDown(self):
        try: os.unlink(self.db)
        except FileNotFoundError: pass

    def _runs(self):
        return {r["id"]: r for r in self.run.list_runs()}

    def _interrupt(self, run_id, ago=3600, executor="exec-dead"):
        with self.run.repo.transaction() as (conn, q):
            r = self.run.repo.read(conn, "runs", run_id, q, lock=True)
            r.update(status="running", executor=executor,
                     lease_until=(datetime.now(timezone.utc) - timedelta(seconds=ago)).isoformat())
            self.run.repo.upsert(conn, "runs", r, q)
            return r

    def _study_runs(self):
        return [r for r in self.run.repo.all_rows("runs") if r["study_id"] == self.study_id]

    def test_interrupted_run_resumes_in_place_with_same_snapshot(self):
        run = self.run.create({"study_id": self.study_id})
        self.assertEqual(run["status"], "completed")
        self._interrupt(run["id"])
        study_before = self.run.repo.get("studies", self.study_id)

        resumed = self.run.retry(run["id"])
        stored = self.run.repo.get("runs", run["id"])

        self.assertEqual(stored["id"], run["id"], "recovery must reuse the run id")
        self.assertEqual(stored["study_id"], self.study_id)
        self.assertEqual(stored["status"], "completed")
        self.assertEqual(stored["recoveries"], 1)
        self.assertEqual(stored["attempt"], 2)
        self.assertEqual(stored["result"]["checksum"], resumed["checksum"])
        self.assertEqual(stored["result"]["borehole"], "SX-01")
        self.assertEqual(self.run.repo.get("studies", self.study_id), study_before)
        self.assertEqual(stored["result"]["model"]["revision"], 1)
        self.assertEqual(len(self._study_runs()), 1, "recovery must not create another run")

    def test_duplicate_create_resumes_but_never_creates_second_run(self):
        first = self.run.create({"study_id": self.study_id})
        self._interrupt(first["id"])
        second = self.run.create({"study_id": self.study_id})
        self.assertEqual(len(self._study_runs()), 1, "a retry must not spawn another run")
        self.assertEqual(second["id"], first["id"])
        self.assertEqual(second["status"], "completed")
        self.assertEqual(self.run.repo.get("runs", first["id"])["recoveries"], 1)

    def test_completed_run_create_keeps_existing_result(self):
        run = self.run.create({"study_id": self.study_id})
        before = self.run.repo.get("runs", run["id"])
        again = self.run.create({"study_id": self.study_id})
        after = self.run.repo.get("runs", run["id"])
        self.assertEqual(again["id"], run["id"])
        self.assertEqual(after["status"], "completed")
        self.assertEqual(after["result"], before["result"])
        self.assertEqual(after["attempt"], before["attempt"])
        self.assertEqual(len(self._study_runs()), 1)

    def test_retry_after_submission_is_rejected(self):
        run = self.run.create({"study_id": self.study_id})
        self.review.create({"run_id": run["id"], "comment": "已核查完整孔段与模型版本，请复核归档。"}, {"email": "analyst@shenxi.test"})
        with self.assertRaises(Exception) as ctx:
            self.run.retry(run["id"])
        self.assertIn("已提审", str(ctx.exception))
        self.assertEqual(self.run.repo.get("runs", run["id"])["status"], "completed")

    def test_review_rejected_run_can_retry_and_keeps_identity(self):
        run = self.run.create({"study_id": self.study_id})
        review = self.review.create({"run_id": run["id"], "comment": "已核查完整孔段与模型版本，请复核归档。"}, {"email": "analyst@shenxi.test"})
        self.review.decide(review["id"], {"status": "rejected", "comment": "请补充说明后重提。"}, {"email": "admin@shenxi.test"})
        self._interrupt(run["id"])
        resumed = self.run.retry(run["id"])
        self.assertEqual(resumed["id"], run["id"])
        self.assertEqual(resumed["status"], "completed")

    def test_retry_after_approval_and_report_is_rejected(self):
        run = self.run.create({"study_id": self.study_id})
        review = self.review.create({"run_id": run["id"], "comment": "已核查完整孔段与模型版本，请复核归档。"}, {"email": "analyst@shenxi.test"})
        self.review.decide(review["id"], {"status": "approved", "comment": "批准归档。"}, {"email": "admin@shenxi.test"})
        created = self.report.create({"title": "沉溪钻孔模型隐状态复核报告", "run_id": run["id"], "review_id": review["id"]})
        with self.assertRaises(Exception):
            self.run.retry(run["id"])
        csv_text = self.report.download(created["id"])
        self.assertIn(run["result"]["checksum"], csv_text)
        self.assertEqual(self.report.download(created["id"]), csv_text, "report output must stay pinned")

    def test_stale_executor_cannot_overwrite_recovered_result(self):
        run = self.run.create({"study_id": self.study_id})
        interrupted = self._interrupt(run["id"], executor="exec-old")
        stale_token, stale_attempt = "exec-old", interrupted.get("attempt", 0)
        recovered = self.run.retry(run["id"])
        recovered_result = self.run.repo.get("runs", run["id"])["result"]

        accepted = self.run._finish(run["id"], stale_token, stale_attempt, result={"checksum": "tampered"})
        self.assertFalse(accepted[1], "late stale executor must be ignored")
        final = self.run.repo.get("runs", run["id"])
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["result"], recovered_result)
        self.assertNotEqual(final["result"].get("checksum"), "tampered")
        self.assertEqual(recovered["id"], run["id"])

    def test_concurrent_duplicate_delivery_yields_one_effective_result(self):
        with self.run.repo.transaction() as (conn, q):
            r = self.run.repo.read(conn, "runs", "run-demo", q, lock=True)
            r.update(status="queued", executor="", attempt=0, result={},
                     lease_until=(datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat())
            self.run.repo.upsert(conn, "runs", r, q)
        outcomes, errors = [], []
        barrier = threading.Barrier(2)

        def deliver():
            try:
                barrier.wait()
                outcomes.append(self.run.execute("run-demo")["status"])
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=deliver) for _ in range(2)]
        for t in threads: t.start()
        for t in threads: t.join()

        self.assertFalse(errors)
        final = self.run.repo.get("runs", "run-demo")
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["attempt"], 1, "exactly one executor should claim the run")
        self.assertEqual(len(outcomes), 2)

    def test_recovery_then_completion_keeps_review_and_report_on_same_result(self):
        run = self.run.create({"study_id": self.study_id})
        self._interrupt(run["id"])
        resumed = self.run.retry(run["id"])
        review = self.review.create({"run_id": run["id"], "comment": "已核查恢复后的完整孔段与模型版本，请复核。"}, {"email": "analyst@shenxi.test"})
        self.review.decide(review["id"], {"status": "approved", "comment": "批准归档。"}, {"email": "admin@shenxi.test"})
        report = self.report.create({"title": "沉溪钻孔模型隐状态复核报告", "run_id": run["id"], "review_id": review["id"]})
        self.assertEqual(self.run.repo.get("runs", run["id"])["result"], self.run.repo.get("runs", resumed["id"])["result"])
        csv_text = self.report.download(report["id"])
        self.assertIn(self.run.repo.get("runs", run["id"])["result"]["checksum"], csv_text)
        self.assertIn("SX-01", csv_text)

    def test_failed_run_can_retry_in_place(self):
        with self.run.repo.transaction() as (conn, q):
            r = self.run.repo.read(conn, "runs", "run-demo", q, lock=True)
            r.update(status="failed", error="模拟执行进程异常", result={}, executor="", lease_until="")
            self.run.repo.upsert(conn, "runs", r, q)
        # Seeded approval blocks this; clear it to model a failure before review.
        for rev in self.run.repo.all_rows("reviews"):
            if rev["run_id"] == "run-demo": rev["status"] = "rejected"; self.run.repo.put("reviews", rev)
        retried = self.run.retry("run-demo")
        self.assertEqual(retried["status"], "completed")
        self.assertEqual(retried["id"], "run-demo")

    def test_queue_unavailable_leaves_run_recoverable(self):
        self.run.TASK_MODE = "redis"
        self.run.REDIS_URL = "redis://127.0.0.1:1/0"
        with self.assertRaises(ValueError):
            self.run.create({"study_id": self.study_id})
        stored = [r for r in self._study_runs()][0]
        self.assertTrue(self.run._lease_expired(stored))
        self.assertIn("Redis", stored["error"])
        self.assertEqual(stored["status"], "queued")

    def test_recover_stale_reenqueues_only_expired_runs_once(self):
        from unittest import mock
        import redis as redis_module

        class FakeRedis:
            def __init__(self):
                self.queue, self.keys = [], {}
            def set(self, name, value, nx=False, ex=None):
                if nx and name in self.keys: return None
                self.keys[name] = value
                return True
            def rpush(self, key, value):
                self.queue.append(value)

        fake = FakeRedis()
        run = self.run.create({"study_id": self.study_id})
        self._interrupt(run["id"])
        other = self.run.repo.get("runs", "run-demo")   # completed, healthy
        with mock.patch.object(redis_module.Redis, "from_url", return_value=fake):
            first = self.run.recover_stale()
            second = self.run.recover_stale()
        self.assertEqual(first, [run["id"]])
        self.assertEqual(second, [], "dedupe key must stop duplicate re-enqueues")
        self.assertEqual(fake.queue, [run["id"]])
        self.assertNotIn("run-demo", fake.queue)
        stored = self.run.repo.get("runs", run["id"])
        self.assertEqual(stored["status"], "queued")
        self.assertEqual(stored["recoveries"], 0, "recovery is counted when claimed")

    def test_healthy_running_run_retry_is_idempotent(self):
        run = self.run.create({"study_id": self.study_id})
        with self.run.repo.transaction() as (conn, q):
            r = self.run.repo.read(conn, "runs", run["id"], q, lock=True)
            r.update(status="running", executor="exec-live", attempt=1,
                     lease_until=(datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat(), result={})
            self.run.repo.upsert(conn, "runs", r, q)
        returned = self.run.retry(run["id"])
        stored = self.run.repo.get("runs", run["id"])
        self.assertEqual(returned["id"], run["id"])
        self.assertEqual(stored["status"], "running")
        self.assertEqual(stored["attempt"], 1, "healthy activation must not be re-claimed")
        self.assertEqual(stored["recoveries"], 0)
        self.assertFalse(returned["retryable"])


if __name__ == "__main__":
    unittest.main()
