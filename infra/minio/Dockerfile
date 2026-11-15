# The upstream historical container tag is no longer a reliable distribution.
# Build the official AGPL-3.0 MinIO release; Go verifies its module checksums.
FROM golang:1.27.4-bookworm AS build
RUN CGO_ENABLED=0 go install github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /go/bin/minio /usr/local/bin/minio
# Retain the upstream image's uid for existing local data volumes.
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
