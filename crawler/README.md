# Crawler 

Code and scripts to construct a containerized Web crawl worker written in Node.js that can be invoked either from the CLI (for testing) or from our `work-dispatcher` (a.k.a.`kpw`).
If non-headless crawls are planned, `Xvfb` (or some X server) should be running in the same host/pod, bound to TCP port 99 (see `misc/Dockerfile.xvfb`).

## Dependencies

* a number NPM packages (`package.json`), chiefly [Puppeteer](https://pptr.dev/)
* a custom build of Chromium with the [VisibleV8](https://github.com/wspr-ncsu/visiblev8) patches applied (to generate VV8 output logs)
* a CLI tool (`vv8-post-processor`, an embedded Go sub-project) to parse/archive the VV8 logs generated during crawls

## Chromium Base Image

The build process (`Makefile`, `Dockerfile`) builds and deploys both the primary crawler project and its post-processor tool dependency into a container image layered on top of an existing Chromium container image.
This base image must contain a working build of Chromium, expected but not strictly required to include the VisibleV8 patches, in the default location `/opt/chromium.org/chromium/chrome` (see the `CHROME_EXE` environment variable setting).
It must also include a JSON dump of Blink/V8's WebIDL files, as can be produced by the VisibleV8 build process with the appropriate option, in the default location `/artifacts/idldata.json` (see the `IDLDATA_FILE` environment variable setting).
The experiment described in the paper was performed using Chromium 80.0.3987.163 built using the [VisibleV8 patches](https://github.com/wspr-ncsu/visiblev8/tree/master/patches/38cc4e3d7a9c060c2214) and build scripts.



