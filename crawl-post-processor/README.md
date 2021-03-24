# Crawl Data Post-Processor

This tool post-processes raw crawl data from MongoDB on a job-by-job basis, loading aggregated and deduplicated "cooked" measurement data into a PostgreSQL database.

It can be built as either a CLI tool or a queue-consuming worker container image (see `Makefile` and `Dockerfile`).  Queue consumption is handled via the `kpw` system (see `work-dispatcher` in the repository root).

## Code Structure

* `main.go`: program entry point
* `config/`: package of vestigial remains of earlier support for multiple different modes of operation
* `mongoz/`: package of utilities for dealing with our MongoDB data structures (some of which include legacies of older projects)
* `syncdb/`: package of obsolete logic for "cooking" crawl data left over from earlier iterations of the project; retained for various utilities and components still used by the latest iteration
* `syncdb2020/`: package of current logic for "cooking" crawl data, as used in the published paper
    * `postgres_schema.sql`: annotated SQL DDL script describing the structure of our data in Postgres

## Data Structure

The `syncdb2020/postgres_schema.sql` file referenced above defines the Postgres database schema used for our project data.  The available data dumps are in this schema format.
