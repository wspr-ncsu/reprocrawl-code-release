# Synchronized Crawl Dispatcher

Our crawls were queued and coordinated using `kpw` (Kubernetes Python Worker), our in-house adapter system for polyglot producer/consumer workflows on our Kubernetes cluster.
We include this snapshot of the `kpw` source code and documentation for completeness.

In our experiment we used the following work queues feeding instances of the `crawler` and `crawl-post-processor` container images described elsewhere in this code release.

* queues serving pods running inside the primary k8s cluster (using the primary/on-site MongoDB instance for raw crawl data storage):
	* `naive-crawl-primary` (headless `crawler` pods)
	* `stealth-crawl-primary` (non-headless `crawler` pods)
	* `vpc-postproc-primary` (`crawl-post-processor` pods pulling from on-site MongoDB and storing final/refined metrics in the central Postgres instance)
* queues serving pods running inside the outpost k8s mini-cluster (using the outpost/off-site MongoDB instance for raw crawl data storage):
	* `naive-crawl-outpost` (headless `crawler` pods)
	* `stealth-crawl-outpost` (non-headless `crawler` pods)
	* `vpc-postproc-outpost` (`crawl-post-processor` pods pulling from off-site MongoDB and storing final/refined metrics in the central Postgres instance via `autossh` tunnel)

Initial jobs (see `experiment-generator` tools) are queued to the `XXX-crawl-YYY` queues with syncronization tags to ensure simulateous launch across all VPs/BCs.  Crawler pods then post completed jobs to the `vpc-postproc-YYY` queues to complete the post-processing of the raw crawl data.

# kpw: Kubernetes Python Worker

*(original `kpw` project documentation)*

## Quickstart

### Building Images
Both the `consumer` and `producer` images can be produced with `make` (in the appropriate subdirectory).  You need GNU Make and Docker available.

### Running Producer (REST API)
Add a new deployment (and service) to your k8s deployment: have the deployment launch the `kpw-producer:VERSION_TAG` image, with the following settings:
* `env`:
   * `RQ_REDIS_URL`: a Redis URL for connecting to your Redis instance (e.g., `redis://192.168.42.255:6279`) 
   * `RQ_QUEUE_NAME`: a single RQ queue name (if you want to enqueue jobs to multiple queues, deploy multiple instances)

The pod will expose a REST API endpoint at port 8080; jobs for endpoint `ENDPOINT` can be queued to the named queue with HTTP POST requests to `http://service-hostname-whatever-that-is:8080/kpw/ENDPOINT` with named keyword arguments passed as a JSON object in the POST body.
**Beware** accidentally passing a *kwarg* to RQ's `enqueue` [function ](http://python-rq.org/docs/) instead of to your actual endpoint this way.  *kwarg* names you need to avoid (unless you actually *want* to do this, which you well might!) include: `job_timeout`, `result_ttl`, `ttl`, `failure_ttl`, `depends_on`, `job_id`, `at_front`, `description`, `args`, and `kwargs`.

### Running Consumer (Pod Adapter)
Add an extra container to your k8s pod spec (`kpw-consumer:VERSION_TAG`) and configure it as follows:
* `env`:
    * `RQ_REDIS_URL`: a Redis URL for connecting to your Redis instance (default: "redis://localhost:6379")
    * `KPW_CONSUMER_HTTP_PORT`: the port on which the worker service listens (on localhost) (default: "8080")
    * `KPW_CONSUMER_READY_PATH`: an HTTP path to probe for worker service readiness (default: None, no probe performed; if provided, the worker service _must_ respond to one of the GET requests to this path with an HTTP 200 response or the consumer will never start pulling jobs from the queue[s])
    * `KPW_CONSUMER_RETRIES`: number of readiness probes to try before aborting and shutting down (default: 5)
    * `KPW_CONSUMER_BACKOFF`: number of seconds to wait between 1st and 2nd readiness probe; doubled each try thereafter (default: 5)
* `args`:
    * one string per queue name to pull from (e.g., `["widgets", "thingermajiggers", "pizzas"]`); defaults to just `default`



## Cookbook

### For Pure-Python Workflows
Ignore **kpw**, mostly, but keep your dispatching compatible with **kpw** (see "Workflow Details") in case you later replace either the producer or consumer with a non-Python component.

### For Non-Python Queue Producers
Add one new deployment/service to your k8s model: an instance of the **kpw** REST API bound to your target Redis and RQ-queue-name.  In your non-Python code, use HTTP POSTs to `http://nameOfYourKpwRestApiService:8080/kpw/ENDPOINT_NAME` with all arguments provided as JSON-encoded key/value pairs in the POST body (**beware** clashing with RQ's `enqueue` function's kwargs).

### For Non-Python Queue Consumers
Modify your consumer logic to be triggered by an HTTP POST at `http://localhost:8080/kpw/ENDPOINT_NAME` with key/value arguments in a JSON-encoded POST body.  The `ENDPOINT_NAME` should match the endpoint specified on the producer side; having it allows different kinds of jobs to coexist in a single RQ queue.

Add the **kpw** consumer container to your pod specification with appropriate configuration for Redis connections and RQ queue names, and it will dispatch queued jobs to your listening HTTP server.  Return an HTTP error status (e.g., 400, 500) to indicate job failure and return HTTP 200 to indicate job success.

## Problem Background
Our primary motivation for using k8s is parallel processing of queued jobs (either in batch or on demand).
Each language ecosystem has its own systems for such processing, e.g.:

* Python: Celery, RQ, Dramatiq, ...
* Ruby: Resque, ...
* Node.js: Kue, Bull, Bee-Queue, ...

Some of these are more robust than others (*I'm looking at you, Node.js*), and none of them interop cleanly.

## Proposed Solution
We propose **kpw** (pronounced "ka-pow", of course, like in the comic books), a minimal producer/consumer system written in Python (our dominant choice for quick-turnaround research systems) using RQ (the simplest robust choice we've found for work queuing) and enabling fire-and-forget polyglot workflows.

In cases where a producer or consumer is written in Python, authors need not use **kpw** directly so long as their job structure is compatible with it (see below).

In cases where a producer is non-Python, a standalone instance of the **kpw** REST API can be used to enqueue jobs using HTTP POST requests from the non-Python app (which can be easily sent from all modern development stacks).

In cases where a consumer is non-Python, the "foreign" app can expose its work-logic as a trivial HTTP REST server (again, straightforward in all modern stacks) which will be invoked by the **kpw** consumer adapter container in a multi-container k8s pod deployment.  I.e.,

* Primary container: non-Python consumer app listening for jobs as HTTP POST requests to localhost:8080
* Adapter container: **kpw** consumer app pulling jobs from RQ and POSTing them to localhost:8080

## Workflow Details
The **kpw** worker module exposes one RQ-invokable function:

```python
def dispatch(endpoint, **kwargs):
    # error handling/etc. left out for clarity
    r = requests.post(f"http://localhost:8080/kpw/{endpoint}", json=kwargs)
    r.raise_for_status()  # convert HTTP 500 and the like to exceptions (failed jobs)
    return r.status_code
```

Python-native consumers can simply adapt the `kpw` module to directly handle the job (without any HTTP monkey business) using a single custom container image.  **Important:** RQ must still be able to invoke `kpw.dispatch` in your Python-native app/container!

Python-native producers can queue tasks to any **kpw**-compatible consumer like this:

```python
from rq import Queue
from redis import Redis

redis_conn = Redis()  # with whatever connection params you need
q = Queue("yourQueueNameHere", connection=redis_conn)

q.enqueue('kpw.dispatch',
    'endpoint_du_jour', # whatever your task is actually named
    arg0="first arg",
    arg1=42,
    arg2="you can actually name these anything you want, so long as they don't collide with RQ's kwargs for enqueue")
```
