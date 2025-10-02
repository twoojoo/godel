# Gödel

An attemp to write a ligthweight kafka-like distributed log in golang.

## Why Go?

My main goal is to learn how to build a distributed log and Golang allows me to bypass a lot of "language logic" by providing great concurrency support, mutexes, etc... When the implementation is done, I may consider rewriting the project in a more performant language.

## Roadmap

- [x] Message serialization and storage
- [x] Log Segments
- [x] Partitions
- [x] Rentention Logic
- [x] Topics
- [x] CLI
    - [x] Run server
    - [x] Create topics
    - [ ] Delete topics
    - [ ] Edit topics
    - [x] Produce
    - [ ] Consume
- [x] Dockerfile (basic)
- [ ] Full concurrency support (mutexes)
- [ ] Empty key partition rotation (round robin)
- [ ] Consumer
- [ ] Write Ahead Log
- [ ] Log segments sparse indexes (faster random access)
- [ ] CRC on messages/batches (corruption detection)
- [ ] Message batching (increased write performance)
- [ ] Fully Distributed
    - [ ] Raft consensus
    - [ ] Replication
- [ ] Golang client library
- [ ] Fully functional CLI
- [ ] Node.js client library
- [ ] Python client library
- [ ] Kubernetes manifests
