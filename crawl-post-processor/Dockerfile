# Start with "compilation" base image (the Go toolchain)
FROM golang:1.13 AS gobuild

# Copy in the source directory and compile there
COPY . /build/
WORKDIR /build/
RUN go build

# Switch to target (stretch with dumb-init)
FROM debian:stretch
RUN apt update && apt install dumb-init
RUN useradd -u 1000 -d /app vpc

# Copy in build artifacts
COPY --from=gobuild /build/vpp /app/bin/

# Default ENV for DB/etc
ENV MONGODB_HOST localhost
ENV MONGODB_PORT 27017
ENV MONGODB_DB not_my_db

# Initialize entrypoint for safe webhook server launch
WORKDIR /app/bin
USER vpc
EXPOSE 8080
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/app/bin/vpp"]
CMD ["syncdb2020-webhook", ":8080"]
