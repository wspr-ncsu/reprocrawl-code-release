import logging
import os
import signal
import sys
import time
import traceback

import redis
import requests
import rq

import barrier

# What Redis to connect to
RQ_REDIS_URL = os.environ.get("RQ_REDIS_URL", "redis://localhost:6379")

# Where (on localhost) the worker HTTP server is
KPW_CONSUMER_HTTP_PORT = int(os.environ.get("KPW_CONSUMER_HTTP_PORT", 8080))
KPW_CONSUMER_URL_BASE = "http://localhost:{0}".format(KPW_CONSUMER_HTTP_PORT)
KPW_CONSUMER_URL_ROOT = "{0}/kpw".format(KPW_CONSUMER_URL_BASE)

# What HTTP path to probe for readiness (if any) and retry/backoff tuning
KPW_CONSUMER_READY_PATH = os.environ.get(
    "KPW_CONSUMER_READY_PATH"
)  # default: None (no readiness probe)
KPW_CONSUMER_BACKOFF = int(os.environ.get("KPW_CONSUMER_BACKOFF", 5))  # seconds
KPW_CONSUMER_RETRIES = int(os.environ.get("KPW_CONSUMER_RETRIES", 5))  # seconds


def dispatch(endpoint, **kwargs):
    """Dispatch task to HTTP worker service on localhost.

    Returns HTTP status-code if a response is received.
    If a connection error is received indicating the worker service
    died or otherwise failed hard, the RQ worker is sent SIGINT
    to trigger RQ worker warm shutdown.
    """
    logger = logging.getLogger("rq.worker.kpw.dispatch")
    url = KPW_CONSUMER_URL_ROOT + "/" + endpoint
    try:
        r = requests.post(url, json=kwargs)
        if not r.ok:
            raise Exception(
                f"Consumer reported error via HTTP {r.status_code} ({r.reason}):\n{r.text}"
            )
        else:
            return r.status_code
    except requests.exceptions.ConnectionError:
        logger.exception("connection error to/from sidecar")
        os.kill(os.getpid(), signal.SIGINT)  # request RQ worker warm-shutdown
        raise  # and let this exception mark the job as "failed"


def probe_readiness(
    ready_url: str,
    retries: int = KPW_CONSUMER_RETRIES,
    backoff: float = KPW_CONSUMER_BACKOFF,
) -> bool:
    """Attempt to HTTP GET <ready_url> up to <retries> times.

    Returns True if a 200 status code was returned for any try,
    otherwise returns False.

    After each failure (unless it was for the last try), sleeps
    <backoff>*2**i, where i is the 0-based try number.

    Logs updates to logger "rq.worker.kpw".
    """
    logger = logging.getLogger("rq.worker.kpw")
    logger.warning(
        "probing for readiness: {0} (backoff={1}, retries={2})".format(
            ready_url, backoff, retries
        )
    )

    for i in range(1, retries + 1):
        failure = None
        try:
            r = requests.get(ready_url)
            if r.status_code == 200:
                logger.info("readiness probe successfull")
                return True
            else:
                failure = "HTTP status: {0}".format(r.status_code)
        except requests.exceptions.ConnectionError as err:
            failure = str(err)
        logger.warning(
            "readiness probe #{0} of {1} failed ({3}): waiting {2} seconds...".format(
                i, retries, backoff, failure
            )
        )
        if i < retries:
            time.sleep(backoff)
            backoff += backoff

    return False


class BarrierSyncWorker(rq.SimpleWorker):
    def __init__(self, queues, bar: barrier.Barrier, *args, **kwargs):
        super().__init__(queues, *args, **kwargs)
        self._barrier = bar

    def execute_job(self, job, queue):
        if "__barrier__" in job.kwargs:
            bsync = job.kwargs["__barrier__"]
            try:
                start = time.time()
                bsync["release_message"] = self._barrier.sync(
                    bsync["tag"],
                    bsync["count"],
                    bsync["message"],
                    timeout=bsync.get("timeout", None),
                )
                stop = time.time()
                bsync["wait_time"] = stop - start
            except barrier.TimeoutError:
                exc_info = sys.exc_info()
                exc_string = super()._get_safe_exception_string(
                    traceback.format_exception(*exc_info)
                )
                super().handle_job_failure(
                    job=job, exc_string=exc_string,
                )
                super().handle_exception(job, *exc_info)
                return False

        return super().execute_job(job, queue)


def main(argv):
    """kpw consumer entry point: handles setup and kicking off queue worker.

    Takes queue names from argv[1:].

    If ENV[KPW_CONSUMER_READY_PATH] is defined, attempts a readiness probe
    before beginning queue consumption.  If the probe fails, aborts the
    process with sys.exit(1).
    """
    rq.logutils.setup_loghandlers()
    logger = logging.getLogger("rq.worker")

    if KPW_CONSUMER_READY_PATH and not probe_readiness(
        KPW_CONSUMER_URL_BASE + KPW_CONSUMER_READY_PATH
    ):
        logger.error("all readiness probes failed; aborting...")
        sys.exit(1)

    queue_names = argv[1:] or ["default"]
    redis_pool = redis.ConnectionPool.from_url(url=RQ_REDIS_URL)
    with barrier.BarrierContext(redis_pool) as bar:
        with rq.Connection(redis.Redis(connection_pool=redis_pool)):
            queues = [rq.Queue(name=qn) for qn in queue_names]
            BarrierSyncWorker(queues, bar).work()


if __name__ == "__main__":
    main(sys.argv)
