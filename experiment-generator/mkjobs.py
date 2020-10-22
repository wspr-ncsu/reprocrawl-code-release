#!/usr/bin/env python3
import argparse
import csv
import copy
import random
import sys
import json
import datetime
import os

import pymongo

# Load environment vars
from dotenv import load_dotenv

load_dotenv()

# Environment knobs
MONGODB_HOST = os.environ.get("MONGODB_HOST", "localhost")
MONGODB_PORT = int(os.environ.get("MONGODB_PORT", 27017))
MONGODB_DB = os.environ.get("MONGODB_DB", "not_my_db")
MONGODB_USER = os.environ.get("MONGODB_USER", None)
MONGODB_PASS = os.environ.get("MONGODB_PASS", None)
MONGODB_AUTHSOURCE = os.environ.get("MONGODB_AUTHSOURCE", "admin")

# Constants
META_CHARS = "@$"


def make_base_job(domain: str, rank: int) -> dict:
    url = f"http://{domain}/"
    doc = {
        "context": {"alexaRank": rank, "rootDomain": domain,},
        "visit": {"url": url,},
        "created": str(datetime.datetime.utcnow()),
        "completed": None,
    }
    return doc


def apply_settings_to_job(job: dict, settings) -> dict:
    job = copy.deepcopy(job)
    for k, v in settings.items():
        if k[0] in META_CHARS:
            continue  # ignore meta-settings (e.g., queue name)
        node = job
        *dot_tree, dot_stem = k.split(".")
        for name in dot_tree:
            try:
                node = node[name]
            except KeyError:
                new_node = {}
                node[name] = new_node
                node = new_node
        node[dot_stem] = v
    return job


def extract_meta_settings(settings: dict, prefix: str) -> dict:
    return {k[1:]: v for k, v in settings.items() if k.startswith(prefix)}


def apply_template_to_job(job: dict, template: dict, meta_values: dict) -> dict:
    computed_settings = {k: v.format(**meta_values) for k, v in template.items()}
    return apply_settings_to_job(job, computed_settings)


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "alexa_file",
        metavar="CSV_FILE",
        type=argparse.FileType("rt", encoding="utf-8"),
        help="CSV file containing Alexa top 1 million domains",
    )
    ap.add_argument("count", metavar="N", type=int, help="how many domains to use")
    ap.add_argument(
        "-r",
        "--random",
        type=int,
        metavar="M",
        default=None,
        help="randomly select M elements from the top N",
    )
    ap.add_argument(
        "matrix_file",
        metavar="JSON_FILE",
        type=argparse.FileType("rb"),
        help="Load experiment matrix settings from JSON_FILE",
    )
    args = ap.parse_args()

    print("parsing job matrix definition...")
    raw_matrix = json.load(args.matrix_file)
    repeats = raw_matrix["generator"]["repeats"]
    global_settings = raw_matrix["generator"]["settings"]
    use_barrier = raw_matrix["generator"].get("barrier", False)
    global_meta = extract_meta_settings(global_settings, "@")
    global_template = extract_meta_settings(global_settings, "$")
    vps = raw_matrix["vantagePoints"]
    bcs = raw_matrix["browserConfigs"]
    matrix = []
    for vp, vcfg in vps.items():
        for bc, bcfg in bcs.items():
            combo = vcfg.copy()
            combo.update(bcfg)
            matrix.append((vp, bc, combo))

    if "tag" in global_meta:
        experiment_tag = global_meta["tag"]
    else:
        # generate a random experiment tag _before_ setting a deterministic random seed
        experiment_tag = format(random.getrandbits(64), "x")

    if "seed" in raw_matrix["generator"]:
        random.seed(raw_matrix["generator"]["seed"])

    print("reading/sampling domains...")
    alexa_domains = [
        (int(rank), domain) for rank, domain in csv.reader(args.alexa_file)
    ]

    chosen_domains = alexa_domains[:args.count]
    if args.random is not None and args.random <= args.count:
        random.shuffle(chosen_domains)
        chosen_domains = chosen_domains[:args.random]

    print("generating job set for experiment from domain sample...")
    jobs = []
    for rep in range(1, repeats + 1):
        global_meta["rep"] = rep
        random.shuffle(chosen_domains)
        for rank, domain in chosen_domains:
            base_job = apply_settings_to_job(
                make_base_job(domain, rank), global_settings
            )
            for vp, bc, config in matrix:
                job = apply_settings_to_job(
                    base_job,
                    {
                        "context.alexaRank": rank,
                        "context.rootDomain": domain,
                        "context.vantagePoint": vp,
                        "context.browserConfig": bc,
                        "context.rep": rep,
                    },
                )
                job = apply_settings_to_job(job, config)

                if use_barrier:
                    job = apply_settings_to_job(
                        job,
                        {
                            "context.barrier.tag": f"sync:{experiment_tag}:{rank}-{domain}-{rep}",
                            "context.barrier.count": len(vps) * len(bcs),
                            "context.barrier.message": f"{rank}-{domain}-{rep}-{vp}-{bc}",
                        },
                    )

                meta = global_meta.copy()
                meta.update(extract_meta_settings(config, "@"))
                template = global_template.copy()
                template.update(extract_meta_settings(config, "$"))
                job = apply_template_to_job(job, template, meta)
                jobs.append(job)

    print(f"uploading jobs to MongoDB @ {MONGODB_HOST}:{MONGODB_PORT}/{MONGODB_DB}...")
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
    db = client[MONGODB_DB]
    jobs_collection = db["jobs"]
    jobs_collection.insert_many(jobs)


if __name__ == "__main__":
    main(sys.argv)
