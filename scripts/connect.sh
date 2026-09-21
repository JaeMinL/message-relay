#!/usr/bin/env bash

grpcurl -plaintext \
  -import-path src/main/proto \
  -proto relay.proto \
  -d @ \
  localhost:50051 relay.v1.MessageRelay/Connect