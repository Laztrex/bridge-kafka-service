# syntax=docker/dockerfile:1

# Images are set at build time. With no value after --build-arg, docker takes it from the environment:
#   BUILDER_IMAGE=... RUNTIME_IMAGE=... docker build --build-arg BUILDER_IMAGE --build-arg RUNTIME_IMAGE .
# BUILDER_IMAGE: Go image for the build; its Go version must satisfy the go directive in go.mod.
# RUNTIME_IMAGE: lightweight runtime image; the binary is static (CGO_ENABLED=0, time zones embedded)
# and needs only sh (for the setup step below) and the system CA roots.
# Configuration is mounted at ./configs (see README); it is never baked into the image.
ARG BUILDER_IMAGE=golang:1.27.3
ARG RUNTIME_IMAGE=alpine:3.21

############################ Builder ############################
FROM ${BUILDER_IMAGE} AS builder

ENV APP_NAME=bridge-kafka-service

USER root

WORKDIR /src

# Integrity comes from the committed go.sum: go mod download checks module hashes against it,
# go mod verify checks the cache content, and -mod=readonly keeps the build from silently editing go.mod/go.sum.
ENV GOSUMDB=off \
    GOFLAGS=-mod=readonly \
    CGO_ENABLED=0 \
    GOMODCACHE=/go/pkg/mod \
    GOCACHE=/root/.cache/go-build

# Dependencies first so they are cached until go.mod/go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download && go mod verify

COPY cmd ./cmd
COPY internal ./internal

# Build metadata (make image passes them); BUILD_DATE is an argument so the build stays reproducible.
# timetzdata embeds the time zone database, so the runtime image needs no tzdata package.
# BUILD_TAGS are extra Go build tags, comma-separated (platform spec lang-go/platform-go-quality): empty is a dev
# build, the release image is built with --build-arg BUILD_TAGS=release.
ARG VERSION=dev
ARG BUILT_BY=docker
ARG COMMIT=none
ARG BUILD_DATE=unknown
ARG BUILD_TAGS=""
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -tags "timetzdata${BUILD_TAGS:+,${BUILD_TAGS}}" -ldflags="-s -w \
    -X main.version=${VERSION} \
    -X main.builtBy=${BUILT_BY} \
    -X main.commit=${COMMIT} \
    -X main.date=${BUILD_DATE}" \
    -o /out/${APP_NAME} ./cmd/${APP_NAME}

############################ Runtime ############################
FROM ${RUNTIME_IMAGE}

ARG RUNTIME_IMAGE

USER root

ENV APP_NAME=bridge-kafka-service \
    LANG=C.UTF-8

LABEL org.opencontainers.image.title="bridge-kafka-service" \
      org.opencontainers.image.base.name="${RUNTIME_IMAGE}"

# Logs and metric files are written relative to the working directory.
WORKDIR /go/src/${APP_NAME}
RUN chown 1001:1001 . && chmod 755 .

COPY --from=builder --chown=1001:1001 /out/${APP_NAME} ./${APP_NAME}

USER 1001

EXPOSE 8081

CMD ["./bridge-kafka-service"]
