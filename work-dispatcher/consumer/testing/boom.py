import sys

import redis
import rq

conn = redis.Redis()
q = rq.Queue(connection=conn)


msg = sys.argv[1] if len(sys.argv) > 1 else "test"
kwargs = {"job_timeout": 3}
if len(sys.argv) > 2:
    fields = sys.argv[2].split(":")
    kwargs["__barrier__"] = {
        "tag": fields[0],
        "count": int(fields[1]),
        "message": fields[2],
        "timeout": float(fields[3]) if len(fields) > 3 else None,
    }

q.enqueue("kpw.dispatch", sys.argv[1] if len(sys.argv) > 1 else "test", **kwargs)
