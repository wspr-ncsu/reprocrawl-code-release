#!/usr/bin/env python3
import argparse
import datetime
import json
import os
import random
import string
import sys
import time

import redis
import rq
import pymongo
from dotenv import load_dotenv

# Load environment vars
load_dotenv()

# Environment knobs
MONGODB_HOST = os.environ.get("MONGODB_HOST", "localhost")
MONGODB_PORT = int(os.environ.get("MONGODB_PORT", 27017))
MONGODB_DB = os.environ.get("MONGODB_DB", "not_my_db")
MONGODB_USER = os.environ.get("MONGODB_USER", None)
MONGODB_PASS = os.environ.get("MONGODB_PASS", None)
MONGODB_AUTHSOURCE = os.environ.get("MONGODB_AUTHSOURCE", "admin")

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

def main(argv):
    ap = argparse.ArgumentParser(description="Queue available jobs.")
    ap.add_argument(
        "-l",
        "--lim",
        nargs="?",
        type=int,
        metavar="NUMBER",
        help="Queue at most NUMBER jobs for processing.",
    )
    ap.add_argument(
        "-f",
        "--filter",
        type=str,
        metavar="JSON_OBJ",
        help="Apply the given JSON_OBJ as a Mongo filter query for selecting jobs",
    )
    args = ap.parse_args(argv[1:])

    print("Connecting to MongoDB at {0}:{1}...".format(MONGODB_HOST, MONGODB_PORT))
    kwargs = dict(host=MONGODB_HOST, port=MONGODB_PORT,)
    if MONGODB_USER is not None:
        kwargs.update(
            dict(
                username=MONGODB_USER,
                password=MONGODB_PASS,
                authSource=MONGODB_AUTHSOURCE,
            )
        )
    client = pymongo.MongoClient(**kwargs)

    print("Opening database '{0}'...".format(MONGODB_DB))
    db = client[MONGODB_DB]
    jobs = db["jobs"]

    query = {"completed": {"$eq": None}}
    if args.filter:
        query = {"$and": [query, json.loads(args.filter)]}
    qcount = jobs.count_documents(query)
    print("There are {0} incomplete jobs to queue.".format(qcount))

    lim = qcount
    if args.lim:
        lim = min(args.lim, qcount)

    if lim <= 0:
        print("Nothing to queue---bye-bye!")
        return

    print("Connecting to Redis at {0}:{1}...".format(REDIS_HOST, REDIS_PORT))
    rconn = redis.Redis(REDIS_HOST, REDIS_PORT)

    yes = input("Queue {0} incomplete jobs ? (yes<enter>)".format(lim))
    if yes != "yes":
        print("'{0}' != 'yes'; aborting...".format(yes))
        return

    print("Queuing, please wait...", end="", flush=True)
    qmap = {}
    sofar = 0
    with rq.Connection(rconn):
        for entry in jobs.find(query).limit(lim):
            context = entry["context"]
            visit = entry["visit"]
            kwargs = dict(
                original_job_id=str(entry["_id"]), context=context, visit=visit
            )

            barrier = context.pop("barrier", None)
            if barrier:
                kwargs["__barrier__"] = barrier

            qname = context.pop("queue")
            try:
                q = qmap[qname]
            except KeyError:
                qmap[qname] = q = rq.Queue(qname)
            q.enqueue("kpw.dispatch", "visitor", **kwargs)
            sofar += 1
            if sofar > 1000:
                print(".", end="", flush=True)
                sofar = 0

    print("DONE!")


if __name__ == "__main__":
    main(sys.argv)
