import logging,time
from pathlib import Path
import redis
from src.config.settings import REDIS_URL, TASK_MODE, RECOVERY_INTERVAL_SECONDS
from src.services.run import execute,recover_stale
from src.utils.repository import init_db
logging.basicConfig(level=logging.INFO)
def main():
    init_db(); client=redis.Redis.from_url(REDIS_URL);last_recovery=0.
    while True:
        Path("/tmp/worker-heartbeat").write_text(str(time.time()))
        try:
            now=time.time()
            if TASK_MODE=="redis" and now-last_recovery>=RECOVERY_INTERVAL_SECONDS:
                last_recovery=now
                recovered=recover_stale()
                if recovered: logging.info("recovered interrupted runs: %s",recovered)
            item=client.blpop("stratum-hidden:jobs", timeout=3)
            if item: execute(item[1].decode())
        except Exception:
            logging.exception("worker failure");time.sleep(2)
if __name__=="__main__": main()
