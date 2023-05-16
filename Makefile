.SECONDARY:
.DELETE_ON_ERROR:
.PHONY: all gen run

all:
	go generate
	go build

gen:
	hub-core-proto --gen=go -o proto proto.toml
	go run github.com/99designs/gqlgen generate

run: all
	./hub-analytics
