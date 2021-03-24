# Towards Realistic and ReproducibleWeb Crawl Measurements

Experiment code and data artifacts released along with paper publication at TheWebConf'21.

## Code Artifacts

We include all original source code and other data essential for reproducing our experiment setup.
Each sub-directory in this repository contains further documentation on its contents.
A summary:

* `crawler`: code to visit URLs and collect/archive raw data
* `crawl-post-processor`: code to post-process raw collected data into aggregate/deduplicated metrics for analysis
* `experiment-generator`: code to process a top-sites list (Alexa, Tranco) and generate batches of crawl jobs for execution
* `misc`: assorted utilities and sidecars to support our primary components working across multiple clusters
* `tranco`: a snapshot of the Tranco top million Web domains as used in the paper's experiment
* `work-dispatcher`: the work queue producing/consuming framework used in our experiment

We do *not* include the Kubernetes/Kustomization resource definition files we used to orchestrate these components as these are highly specific to our infrastructure.
As deployed, they contained significant private information, and the redaction required would render them both undeployable and unenlightening.

## Dataset Snaphot

A compressed PostgreSQL database backup script containing all the post-processed data from the spring 2020 collection experiment described in the paper is [available here](https://drive.google.com/file/d/1x-6_ATLtUTgBGmF8Yuz_ne-e_G20ve46/view?usp=sharing).

