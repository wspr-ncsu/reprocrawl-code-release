# Miscellaneous Infastructure Pieces

## GOST Tunnel On-Ramp

We directed crawl traffic through our cloud VP using an encrypted SOCKS5 tunnel provided by [GO Simple Tunnel (GOST)](https://github.com/ginuerzh/gost).
The `cloud-gost-onramp/Dockerfile` container image is deployed inside the primary k8s cluster to provide a SOCKS5 server that cloud VP crawlers use to forward Web traffic.

This on-ramp service forwards traffic through an encrypted KCP tunnel (see the `kcp.config` file) to the cloud point-of-presence running on a cloud VPS (Amazon EC2 instance in our case).  The cloud instance of GOST is run using the same `kcp.config` file but without any "ChainNodes" configured as it is the end of the tunnel chain.

## Xvfb Sidecar

Non-headless crawler pods included an extra "sidecar" container running `Xvfb` on TCP port 99 to allow full-Chromium to render its output without a full desktop environment.

`Dockerfile.xvfb` specifies this container image.  It needs no special runtime configuration, it simply needs to run in the same pod as the non-headless `crawler` container instance.

## AutoSSH Container

We used persistent, long-lived SSH tunnels to give pods in the outpost k8s cluster access to our central Redis server (coordinating work queues) and Postgres database (post-processed result data).

`Dockerfile.autossh` specifies a container running `autossh` in a robust, non-interactive manner.  We deployed it inside the primary k8s cluster to maintain reverse-SSH tunnels from the outpost k8s mini-cluster. 

SSH private keys are provided via k8s secrets mounted on the `/secrets` container volume.

Specific tunnel configuration is provided via command-line arguments appended to the `ENTRYPOINT` in the k8s pod container specification.
