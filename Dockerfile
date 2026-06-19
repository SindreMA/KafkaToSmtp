# syntax=docker/dockerfile:1

# --- build stage ---------------------------------------------------------
FROM golang:1.23-alpine AS build
WORKDIR /src

# Resolve modules at build time. No committed go.sum is required; -mod=mod lets
# the build fetch and verify dependencies against the public checksum database.
ENV CGO_ENABLED=0 GOOS=linux GOFLAGS=-mod=mod

COPY go.mod ./
RUN go mod download

COPY *.go ./
RUN go build -trimpath -ldflags="-s -w" -o /out/kafka-to-smtp .

# --- runtime stage -------------------------------------------------------
# distroless static: ~2 MB, no shell, includes CA certs, runs as nonroot (65532).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kafka-to-smtp /kafka-to-smtp
USER nonroot:nonroot
ENTRYPOINT ["/kafka-to-smtp"]
