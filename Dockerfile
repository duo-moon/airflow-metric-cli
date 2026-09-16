# syntax=docker/dockerfile:1.7

# ---------- build stage ----------
FROM golang:1.26-alpine AS builder

# git is needed for VCS stamping (`go build` reads .git for buildinfo).
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Cache module downloads separately from source for faster iteration.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w \
      -X github.com/duo-moon/airflow-metric-cli/internal/version.Version=${VERSION} \
      -X github.com/duo-moon/airflow-metric-cli/internal/version.Commit=${COMMIT} \
      -X github.com/duo-moon/airflow-metric-cli/internal/version.Date=${DATE}" \
    -o /out/afmetric ./cmd/afmetric

# ---------- runtime stage ----------
# distroless/static ships CA certs + tzdata; nonroot user has uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/afmetric /usr/local/bin/afmetric

USER nonroot:nonroot

# TUI (`run`) needs a real TTY — pass -it when invoking `docker run`.
# Headless commands (`ping`, `version`) work without one.
ENTRYPOINT ["/usr/local/bin/afmetric"]
CMD ["--help"]
