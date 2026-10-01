# Java Distributed File Store

A multi-threaded distributed file-storage system implemented in Java using TCP sockets.

Originally developed for the University of Southampton **COMP2207 – Distributed Systems and Networks** module.

**Coursework mark: 78/100**

## Overview

This project implements a small distributed storage system consisting of a central **Controller** and multiple independent **Dstore** nodes.

Clients communicate with the Controller to store, load, list and remove files. The Controller coordinates file placement and replication across the available Dstores, while the Dstores are responsible for storing and serving the actual file data.

The project was designed to explore distributed-systems concepts including:

- TCP socket communication
- Multi-threaded request handling
- File replication
- Concurrent client operations
- Synchronisation between distributed components
- Timeouts and acknowledgement protocols
- Storage-node failure tolerance
- Distributed file metadata
- Retry behaviour across replicas

## Architecture

```text
                      ┌──────────────┐
                      │  Controller  │
                      │              │
                      │ File index   │
                      │ Replication  │
                      │ Coordination │
                      └──────┬───────┘
                             │ TCP
                 ┌───────────┼───────────┐
                 │           │           │
            ┌────▼────┐ ┌────▼────┐ ┌────▼────┐
            │ Dstore  │ │ Dstore  │ │ Dstore  │
            │   #1    │ │   #2    │ │   #3    │
            └─────────┘ └─────────┘ └─────────┘
```

### Controller

The Controller maintains the logical state of the distributed datastore.

It:

- accepts concurrent TCP connections;
- tracks connected Dstores;
- maintains file names, sizes and storage state;
- selects storage nodes for replicated files;
- coordinates `STORE` and `REMOVE` operations;
- waits for acknowledgements from multiple Dstores;
- directs clients to replicas for `LOAD` requests;
- supports `RELOAD` requests when another replica should be tried;
- reports errors when too few Dstores are available;
- contains a partial implementation of the rebalancing workflow.

### Dstore

Each Dstore represents an independent storage node.

A Dstore:

- registers itself with the Controller;
- accepts concurrent client connections;
- stores binary files on its local filesystem;
- returns files directly to clients;
- removes files when instructed by the Controller;
- reports successful operations using acknowledgement messages;
- reports its currently stored files to the Controller.

## Protocol

Communication uses a simple text-based protocol over TCP.

The implementation defines messages including:

- `JOIN`
- `LIST`
- `STORE`
- `STORE_TO`
- `STORE_ACK`
- `STORE_COMPLETE`
- `LOAD`
- `LOAD_FROM`
- `LOAD_DATA`
- `RELOAD`
- `REMOVE`
- `REMOVE_ACK`
- `REMOVE_COMPLETE`
- `REBALANCE`
- `REBALANCE_STORE`
- `REBALANCE_COMPLETE`
- `ERROR_FILE_ALREADY_EXISTS`
- `ERROR_FILE_DOES_NOT_EXIST`
- `ERROR_NOT_ENOUGH_DSTORES`
- `ERROR_LOAD`

File contents themselves are transferred as binary data.

## Concurrency

Both the Controller and Dstores create separate handler threads for incoming connections, allowing multiple clients and storage nodes to interact with the system concurrently.

The Controller uses synchronised operations for shared state and `CountDownLatch` objects to coordinate acknowledgements from replicated Dstores before `STORE` and `REMOVE` operations are considered complete.

## Replication and Failure Handling

Files are replicated across multiple Dstores according to a configurable replication factor.

The Controller records which Dstores contain each file and can direct `LOAD` requests to alternative replicas through the `RELOAD` protocol.

The submitted implementation successfully passed automated tests covering:

- sequential `STORE`, `LOAD`, `LIST` and `REMOVE` operations;
- protocol error handling;
- replicated storage;
- concurrent client access;
- concurrent `STORE` and `LOAD` operations;
- operation with multiple failed Dstores while enough replicas remained available.

## Coursework Results

The implementation received **78/100**.

### Mark breakdown

| Section | Result |
| --- | ---: |
| Sequential protocol behaviour | 50/50 |
| Replication and file distribution | 5/10 |
| Concurrent requests | 8/10 |
| Single-Dstore failure tolerance | 5/10 |
| Failure tolerance up to `N - R` Dstores | 10/10 |
| Dynamic rebalancing | 0/10 |
| **Total** | **78/100** |

## Known Limitations

This repository represents the coursework implementation rather than a production-ready distributed filesystem.

The automated assessment identified several incomplete or incorrect behaviours:

- file placement can select one additional Dstore in some configurations;
- some concurrent `REMOVE` cases are unreliable;
- failure detection is incomplete in one tested configuration;
- dynamic rebalancing when Dstores join or leave was not completed.

The rebalancing workflow is partially present in the Controller, but the Dstore-side rebalance handler is unfinished.

## Project Structure

```text
Controller.java
    Coordinates clients, replicas and distributed file state.

Dstore.java
    Storage-node server responsible for storing and serving files.

Index.java
    Maintains file names, sizes and operation states.

Protocol.java
    Defines protocol message constants shared by the components.
```

## Running

Compile the source files:

```bash
javac *.java
```

Start the Controller:

```bash
java Controller <controllerPort> <replicationFactor> <timeoutMs> <rebalancePeriod>
```

Example:

```bash
java Controller 12345 3 1000 10000
```

Start one or more Dstores:

```bash
java Dstore <port> <controllerPort> <timeoutMs> <storageDirectory>
```

Example:

```bash
java Dstore 12346 12345 1000 ./dstore1
java Dstore 12347 12345 1000 ./dstore2
java Dstore 12348 12345 1000 ./dstore3
```

## What I Learned

This project introduced me to the practical challenges of distributed software, particularly coordinating state across independent processes while handling concurrency, replication, failures and network communication.

It also highlighted how race conditions, failure handling and consistency become substantially harder to reason about once multiple clients and storage nodes operate concurrently.

## Notes

This was an assessed university coursework project. The code is preserved largely in its submitted form so that the repository reflects the implementation that was actually graded.
