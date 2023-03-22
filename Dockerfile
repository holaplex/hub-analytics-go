FROM rustlang/rust:nightly AS build-hub-core-proto

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

WORKDIR /app
# Install hub-core-proto
RUN git clone https://github.com/holaplex/hub-core

WORKDIR /app/hub-core

RUN cargo build --bin hub-core-proto --features clap
RUN ls target/
RUN cp target/debug/hub-core-proto .

FROM golang:1.19-bullseye AS build-base

WORKDIR /app

# Copy only files required to install dependencies (better layer caching)
COPY go.mod go.sum ./

# Use cache mount to speed up install of existing dependencies
RUN --mount=type=cache,target=/go/pkg/mod \
  --mount=type=cache,target=/root/.cache/go-build \
  go mod download

FROM build-base AS build-production

# Get Protoc
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive \
    apt-get install --no-install-recommends --assume-yes \
      protobuf-compiler \
      golang-goprotobuf-dev

# Get hub-core-proto binary
COPY --from=build-hub-core-proto /app/hub-core/hub-core-proto /usr/local/bin/hub-core-proto

#RUN protoc proto/analytics.proto --go_out=proto --go_opt=Mproto/analytics.proto=/analytics

# Add non root user
RUN useradd -u 1001 nonroot

COPY . .

# Compile application during build rather than at runtime
# Add flags to statically link binary
RUN go build \
  -ldflags="-linkmode external -extldflags -static" \
  -tags netgo \
  -o hub-analytics

# Use separate stage for deployable image
FROM scratch

WORKDIR /

COPY --from=build-production /etc/passwd /etc/passwd

# Copy the app binary from the build stage
COPY --from=build-production /app/hub-analytics hub-analytics

# Use nonroot user
USER nonroot

CMD ["/hub-analytics"]
