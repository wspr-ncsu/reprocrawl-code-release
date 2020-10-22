#!/usr/bin/env python3
import json
import sys

import redis
import rq

endpoint = sys.argv[1]
job_args = json.loads(sys.argv[2])

with redis.Redis() as conn:
    q = rq.Queue(connection=conn)
    q.enqueue("kpw.dispatch", endpoint, **job_args)
