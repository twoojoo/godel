# Godel

An attemp to write a ligthweight kafka-like distributed log in golang.

## Why Go?

My main goal is to learn how to build a distributed log and Golang allows me to bypass a lot of "language logic" by providing great cuncurrency support, mutexes, etc... When the implementation is done, I may consider rewriting the project in a more performant language.

## Roadmap

- [x] Message serialization and storage
- [x] Log Segments
- [x] Partitions
- [x] Rention Logic
- [x] Topics
- [x] CLI
    - [x] Run server
    - [x] Create topics
    - [ ] Delete topics
    - [ ] Edit topics
    - [x] Produce
    - [ ] Consume
- [x] Dockerfile
- [ ] Empty key partition rotation
- [ ] Consumer
- [ ] Write Ahead Log
- [ ] Log segments sparse indexes
- [ ] CRC on messages/batches
- [ ] Message batching
- [ ] Fully Distributed
    - [ ] Raft consensus
    - [ ] Replication
- [ ] Golang client library
- [ ] Fully functional CLI
- [ ] Node.js client library
- [ ] Python client library
- [ ] Kubernetes manifests
