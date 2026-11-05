# Local stack

Everything runs on a laptop with Docker (Compose v2.20+) and no cloud account.
Docker Desktop should be given at least 6 GB; the full stack stays under 5 GB of
configured memory limits (measured idle usage is about 1.1 GB core, 1.6 GB with lineage).

```bash
make demo            # up + wait healthy + smoke test + print URLs
make demo LINEAGE=1  # same, plus Marquez lineage UI
make ps              # status and health
make down            # stop (keeps data); `make clean` also deletes volumes
```

Credentials are local-dev defaults from `.env` (`make up` copies `.env.example` on first run).
Never reuse them anywhere real.

## Services

| Service | Image | Host port | Purpose | Memory limit |
|---|---|---|---|---|
| minio | Chainguard MinIO (digest-pinned) | 9000 (S3), 9001 (console) | S3-compatible lake storage | 512 MB |
| minio-init | Chainguard mc (digest-pinned) | - | one-shot: creates `bronze`, `silver`, `gold` buckets | 128 MB |
| redpanda | redpandadata/redpanda v24.3.11 | 19092 (Kafka), 9644 (admin) | Kafka-compatible broker | 768 MB |
| redpanda-console | redpandadata/console v2.8.5 | 8080 | topic browser | 256 MB |
| spark-master | apache/spark 3.5.5 (Java 17) | 8081 (UI), 7077 | Spark standalone master | 768 MB |
| spark-worker | apache/spark 3.5.5 | 8082 (UI) | executor host, 2 cores / 1 GB | 1.5 GB |
| postgres | postgres 16.8 | 5432 | serving and metadata DB | 256 MB |
| marquez-db* | postgres 16.8 | - | Marquez backing store | 192 MB |
| marquez* | marquezproject/marquez 0.50.0 | 5002 (API), 5003 (admin) | OpenLineage backend | 640 MB |
| marquez-web* | marquezproject/marquez-web 0.50.0 | 3002 | lineage UI | 192 MB |

\* compose profile `lineage` (opt in with `LINEAGE=1` or `docker compose --profile lineage up -d`).

Host ports are overridable in `.env`. All published ports bind to `127.0.0.1` only (see Security). Marquez defaults avoid 5000 (macOS AirPlay) and 3000 (common dev servers).

## Verify

`make smoke` (or `bash tests/stack/smoke.sh`, `LINEAGE=1` to include Marquez) probes each endpoint:
MinIO health and the three buckets, Redpanda admin and console, Spark master and worker UIs plus worker
registration (`ALIVE`), and Postgres readiness. Every long-running service also has a Docker health check;
`scripts/wait-healthy.sh` blocks until all report healthy (used by `make up`).

Manual spot checks: MinIO console at http://localhost:9001 shows three buckets; Spark UI at
http://localhost:8081 shows one ALIVE worker.

## Security: loopback binding

The stack ships default passwords (MinIO, Postgres, Marquez DB), so every published port is bound to
`${BIND_ADDR:-127.0.0.1}` and is not reachable from the LAN. Redpanda's advertised external Kafka address is
`localhost:19092`, which is correct for host clients. To expose the stack deliberately (for example to a
second machine), set `BIND_ADDR=0.0.0.0` in `.env` and change every default password first; remote Kafka
clients would also need the advertised address changed.

## Image pins

Every image is pinned by multi-arch index digest (`image: name@sha256:... # readable-tag`). `make verify-pins`
fails if any `image:` lacks a digest. To refresh a pin:

```bash
docker buildx imagetools inspect <image:tag>   # copy the top-level "Digest:" (the index, not a platform manifest)
# edit docker-compose.yml: image: <name>@sha256:<digest>  # <tag>
make verify-pins && docker compose --profile lineage pull && make demo LINEAGE=1
```

## Notes and gotchas

- MinIO no longer publishes images to Docker Hub or Quay. The stack uses Chainguard's builds pinned by
  digest (the `-dev` variant for the server so a shell and `wget` exist for the health check). Swapping to
  another S3 implementation only needs the endpoint in `.env`; the lake code addresses it via S3 API.
- Spark uses the official `apache/spark` image because Bitnami's free images were withdrawn.
- `marquez-web` is published for amd64 only, so it runs under emulation on Apple Silicon.
- `docker compose up --wait` treats the exited `minio-init` job as a failure, hence `scripts/wait-healthy.sh`.

## Roadmap hooks

`make demo` prints TODOs that later tickets replace: #5 generator, #6 bronze, #7 silver, #8 gold,
#11 streaming, #24 lineage, #31 final 10-minute demo.
