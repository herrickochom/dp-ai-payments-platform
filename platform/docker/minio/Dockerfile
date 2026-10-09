FROM debian:bookworm-slim

ARG MINIO_RELEASE=RELEASE.2025-09-07T16-13-09Z

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && curl --fail --show-error --location \
      "https://dl.min.io/server/minio/release/linux-amd64/minio.${MINIO_RELEASE}" \
      -o /usr/local/bin/minio \
 && curl --fail --show-error --location \
      "https://dl.min.io/server/minio/release/linux-amd64/minio.${MINIO_RELEASE}.sha256sum" \
      -o /tmp/minio.sha256sum \
 && cd /usr/local/bin \
 && printf '%s  %s\n' \
      "$(awk '{print $1}' /tmp/minio.sha256sum)" \
      "minio" | sha256sum -c - \
 && chmod 0755 /usr/local/bin/minio \
 && rm -rf /var/lib/apt/lists/* /tmp/minio.sha256sum

ENTRYPOINT ["minio"]
