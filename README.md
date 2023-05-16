# `hub-analytics`

Write and query Hub project analytics using InfluxDB.

## Getting Started

**Dependencies**

- [`hub-core-proto`](https://github.com/holaplex/hub-core/tree/stable/crates/core-build/src/bin)
- Docker Compose
- Make

Installing `hub-core-proto`:
```sh
cargo install --locked holaplex-hub-core-build --bin hub-core-proto \
  --git 'https://github.com/holaplex/hub-core' --branch stable --features clap
```

**Building and Running**

To build the project simply run `make build`, which will handle running
`go generate` and `go build`.  To run the compiled binary, execute the
following:

```sh
docker-compose up -d
./hub-analytics
```

## Future Work

- The query layer, located in `graph/schema.resolvers.go`, is as yet unfinished.
- The performance characteristics of the schema currently used to log records
  to InfluxDB has not been tested.
- The GraphQL parser currently does not appear to avoid excess resolver calls or
  N+1 queries.
- The exact provisioning process for this service and its associated resources
  is still unclear.
