ARG BASE_IMAGE=node:lts-stretch

# Start with "compilation" base image (the Go toolchain)
FROM golang:1.13 AS gobuild

# Copy in the vv8-post-processor directory and compile there
COPY vv8-post-processor/ /build/
WORKDIR /build/
RUN go build

# Now, switch to "production" base image (which must be based on node:lts-XXX)
FROM $BASE_IMAGE

# Environmental tunables
ENV \
	WEBHOOK_HTTP_PORT=3000 \
	CHROME_EXE=/opt/chromium.org/chromium/chrome \
	NOT_HEADLESS=false \
	WITH_SANDBOX=false \
	VV8PP_EXE=/home/node/bin/vv8-post-processor \
	IDLDATA_FILE=/artifacts/idldata.json

# DB tunables needed to be exposed
ENV \
	MONGODB_HOST=localhost \
	MONGODB_PORT=27017 \
	MONGODB_DB=not_my_db \
	MONGODB_AUTHDB=admin \
	PGHOST=localhost \
	PGPORT=5432 \
	PGUSER=jane_doe \
	PGPASSWORD=never_gonna_give_you_up \
	PGDATABASE=not_my_db

# Sanity check--abort the build if there's no $IDLDATA_FILE
RUN ["/bin/sh", "-c", "test -r \"$IDLDATA_FILE\""]

# Get the "tini" init system from the Github release URL (alas, sans gpg signature verification)
USER root
WORKDIR /opt/bin
RUN wget -O /opt/bin/tini https://github.com/krallin/tini/releases/download/v0.18.0/tini-amd64 && chmod +x /opt/bin/tini

# Copy in the build artifacts from Go
COPY --chown=node:node --from=gobuild /build/vv8-post-processor /home/node/bin/

# Copy in Node.js app
COPY --chown=node:node . /home/node/app/

# Deploy the express app
WORKDIR /home/node/app
USER node
RUN npm install --production

ENTRYPOINT ["/opt/bin/tini", "./index.js", "webhook"]
