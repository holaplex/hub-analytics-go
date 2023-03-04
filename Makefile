.SECONDARY:
.DELETE_ON_ERROR:
.PHONY: all proto run

all:
	go generate
	go build

proto:
	hub-core-proto --gen=go -o proto proto.toml

run: all
	./hub-analytics
