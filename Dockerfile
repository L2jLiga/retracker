# syntax=docker/dockerfile:1.9
# ─── Build stage ─────────────────────────────────────────────────────────────
FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /src

# Cache dependencies separately from source
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/root/.cache/go \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH} \
    go build \
      -trimpath \
      -ldflags="-s -w \
        -X main.Version=${VERSION} \
        -X main.Commit=${COMMIT} \
        -X main.BuildDate=${BUILD_DATE}" \
      -o /retracker \
      ./cmd/retracker

# ─── Final stage — Google distroless static (no shell, minimal attack surface)
FROM gcr.io/distroless/static:nonroot AS final

LABEL org.opencontainers.image.title="retracker" \
      org.opencontainers.image.description="Multi-interface BitTorrent re-tracker (BEP 3/15/23)" \
      org.opencontainers.image.source="https://github.com/l2jliga/retracker" \
      org.opencontainers.image.licenses="MIT"

COPY --from=builder /retracker /retracker

# Default config location (override via volume or env)
COPY config.example.yaml /etc/retracker/config.yaml

# HTTP tracker / UDP tracker / metrics / health
EXPOSE 6969/tcp 6969/udp 9090/tcp 8080/tcp

USER nonroot:nonroot

# Healthcheck via the /healthz endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/retracker", "-healthcheck"]

ENTRYPOINT ["/retracker"]
CMD ["-config", "/etc/retracker/config.yaml"]
