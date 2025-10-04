# Gödel

An attemp to write a ligthweight kafka-like distributed log in golang.

## Why Go?

My main goal is to learn how to build a distributed log and Golang allows me to bypass a lot of "language logic" by providing great concurrency support, mutexes, etc... When the implementation is done, I may consider rewriting the project in a more performant language.

## Roadmap

- [x] Message serialization and storage
- [x] Log Segments
- [x] Partitions
- [x] Rentention Logic
    - [x] time (retention.ms)
    - [x] size (retention.bytes)
    - [ ] cleanup policies
        - [x] delete
        - [ ] compact (latest message per key)
- [x] Topics
- [x] TCP protocol
- [x] Concurrent request handling
- [ ] Commands 
    - [x] Run server
    - [ ] Edit server
    - [x] Create topics
    - [ ] Get topic
    - [x] List topics
    - [x] Delete topics
    - [ ] Edit topics
    - [x] Produce
    - [x] Consume
    - [x] Commit
    - [x] List consumer groups
    - [ ] Get consumer group
    - [ ] Create consumer group
    - [ ] Delete consumer group
    - [x] Create consumer
    - [x] Delete consumer
- [x] Consumer groups
- [x] Consumer rebalancing
- [x] Consumer heartbeats
- [x] Autocommit
- [x] Consumer groups persistence
- [ ] Rebalancing notif to consumers
- [ ] Full concurrency support (mutexes)
- [ ] Asynchronous topic deletion
- [ ] Empty key partition rotation (round robin)
- [ ] Write Ahead Log
- [ ] Log segments sparse indexes (faster random access)
- [ ] CRC on messages/batches (corruption detection)
- [ ] Message batching (increased write performance)
- [ ] Fully Distributed
    - [ ] Raft consensus
    - [ ] Partitions distribution
    - [ ] Consumer groups distribution
    - [ ] Replication
    - [ ] Disaster recovery
- [ ] Golang client library
- [ ] Fully functional CLI
- [ ] Node.js client library
- [ ] Python client library
- [x] Dockerfile (basic)
- [ ] Kubernetes manifests

## To do

- [x] decouple consumer creation and consume action
- [x] simplify protocol files and make them public
