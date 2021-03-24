# Experiment Generation/Queueing Tools

The scripts and data files in this directory generate and queue the stream of synchronized crawl jobs to collect our measurements.

## Crawl Configuration Templates

The `mkjobs.py` script takes as input a Tranco (or Alexa) list of top domain rankings and a JSON template describing how many job variants are to be constructed for each domain.
The generator template logic is unfortunately somewhat complex, as it evolved ad hoc features to support a wide array of tests and experiments over the course of the project.

The basic generator schema is as follows:

* `generator`: general rules for creating jobs for each input domain
    * `seed`: arbitrary string used to seed Python's RNG for randomly shuffling input domain order
    * `repeats`: integer number of top-level experiment repetitions to run
    * `barrier`: boolean (synchronize or don't synchronize parallel job sets)
    * `settings`: key/value mapping providing a starting template for job object values
        * `A.B.C`: arbitrary value to store in each job object under `job[A][B][C]`
        * `@name`: arbitrary value to store under "meta" key `name` (not in the generated job; used to subsequently populate `$...` template fields)
        * `$...`: string template (e.g., `{bc}-crawl-{site}` to evaluate/populate using current meta field values (e.g., `@bc` and `@site`)
* `vantagePoints`: key/value mapping of VP names to custom settings overrides
    * `VP_NAME`: settings object like `generator.settings` above
    * `...`
* `browserConfigs`: key/value mapping of BC names to custom settings overrides
    * `BC_NAME`: settings object like `generator.settings` above
    * `...`

Included generation templates include:

* `bandwidth-throttling/`: pair of preliminary test configurations, one `with-throttling` and one `without-throttling`
* `loiter-time-test/`: set of preliminary test configurations performing crawls with increasingly long loiter times (e.g., `matrix-15.json`, `matrix-20.json`, etc.)
* `primary-experiment/`: the master configuration matrix for the primary data collection experiment described in the paper, with an automation script demonstrating proper usage
 

## Jobs

Each individual planned URL visit is represented as a `job` record in the MongoDB instance being used for raw data collection.
`Jobs` comprise all the configuration settings and metadata for that job (VP, BC, timing, headlessness, etc.), and life-cycle metadata.
The lifecycle metadata starts empty (status: `created`) and is progressively filled out with state-changes and timestamps as the job is queued, started, and hopefully completed successfully.
The final states of all `job` objects can be aggregated to compute whole-experiment success/failure rates.

### Job Schema

* `context`: contextual job metadata
    * `alexaRank`: Alexa/Trank site domain rank number
    * `rootDomain`: domain as listed in Alexa/Tranco list
    * `vantagePoint`: name of VP used
    * `browserConfig`: name of BC used
    * `rep`: which visit repetition we are on (1, 2, ...)
    * `barrier`: synchronization data used to coordinate the launch of parallel visits
        * `tag`: all jobs containing *this* string will be synchronized together
        * `count`: number of jobs that should have this `tag` (required for barrier turnstile)
* `visit`: specific job parameters
    * `url`: target URL to visit
    * `navTimeout`: max timeout for initial page navigation (seconds)
    * `pageLoiter`: how long to spend on a page after initial navigation completes (seconds)
    * `proxy`: SOCKS5 proxy to use (OPTIONAL)
        * `host`: hostname/IP address
        * `port`: TCP port number
    * `networkConfig`: Chromium network throttling settings to use (OPTIONAL)
        * `minRTT`: latency floor (in milleseconds)
        * `maxUp`: upload bandwidth limit (in octets/second)
        * `maxDown`: download bandwidth limit (in octets/second)
* `created`: timestamp of job creation
* `complete`: end-status of job (`null` until job starts running)
    * `status`: state in which job ended
    * `when`: timestamp of completion (whether success/failure)

### Workflow Warts

The scar-tissue of unanticipated software evolution is evident in this directory.
The workflow is 2-stage: first `mkjobs.py` creates new `job` records in MongoDB, then `qjobs.py` finds not-yet-completed `jobs` in MongoDB and queues them in Redis for execution by queue-consuming workers.
This approach was intended to facilitate job-retries for jobs that failed and to minimize the amount of data that needed to be stored in the Redis queues (e.g., a `job` ID rather than the whole `job` record).

Network resource constraints eventually forced us to dedicate independent MongoDB instances for the primary and outpost k8s clusters.
Rather than further complicate the `mkjobs.py` process to create `job` records in two different MongoDB instances, we began simply queuing the full `job` record in Redis and creating the `job` records on-demand (if necessary) when the `job` was dequeued for processing.
This effectively breaks the "queue remaining un-processed jobs" idea, as the primary MongoDB `job` collection will contain never-to-be-completed records for all the outpost `jobs` that are demand-created and marked completed in the *outpost* MongoDB instance.


