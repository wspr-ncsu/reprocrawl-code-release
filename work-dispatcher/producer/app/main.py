import os

import redis
import rq

from flask import Flask, request

app = Flask(__name__)

RQ_REDIS_URL = os.environ.get("RQ_REDIS_URL", "redis://localhost:6379")

active_conn = redis.Redis.from_url(RQ_REDIS_URL)


@app.route("/<queue>/kpw/<endpoint>", methods=["POST"])
def enqueue(queue, endpoint):
    r_queue = rq.Queue(name=queue, connection=active_conn)
    args = request.get_json()
    job = r_queue.enqueue("kpw.dispatch", endpoint, **args)
    return str(job.id)
