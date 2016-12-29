# Jiraffet

## TODO

* jiraffetdb - receiving handshake needs a timeout.
* jiraffetdb - sending messages needs a limited queue, not the OS send buffer.


## Properties

### JiraffetDB Application

    # If set, puts the Raft database in the given directory.
    # If not set, this defaults to ${jireffedb.home}/raftdb.
    jiraffetdb.raft.db=
     
    # If set, puts the Key-value database in the given directory.
    # If not set, this defaults to ${jireffedb.home}/kvdb.
    jiraffetdb.kv.db=
     
    jiraffetdb.home=${user.home}/.jiraffetdb
    jiraffetdb.nodes=jiraffet://localhost:9001,jiraffet://localhost:9002,jiraffet://localhost:9003,jiraffet://localhost:9004,jiraffet://localhost:9005
    jiraffetdb.this.node=auto

## Design

The [Raft] algorithm, despite its simplicity, has necessary complexity.
To manage this complexity Jiraffet makes some design choices about how
and where to put algorithmic concerns.

This breakup is, generally

1. Communication, Network.
2. Data Storage.
3. The Raft Algorithm.

### Communication, Network

First, the network layer is specified by the `JiraffetIO` interface. The IO layer must maintain a list
of who is currently in the cluster and how to establish connections. It avoids imposing any requirements on the
data sent, such as a type.

As a result of this design decision, cluster membership changes must be handled at the layer above the
Raft algorithm. This makes implementing lower-level logic, like storage, more simple, but does impose some 
requirement on the caller to implement features like cluster membership.

### Data Storage

Data storage is intentionally very simplistic. It is not strictly store-retrieve
(or CRUD, create, read, update, delete) as some storage operations do require a measure of logic. Every
record stored has two states, _committed_ or _applied_. A record written to a leader's storage is
committed. This implies that a majority of followers have also committed the record, and so it will
eventually be applied. A log written to a follower's storage may or may not be applied depending on 
if the leader reports that a majority of followers have written it.

See the `LogDao` interface for details.

### The Raft Algorithm

Finally, the actual Raft algorithm is implemented by the `Jiraffet` class. It ties together the
network and storage objects using the `JiraffetIO` class to mutate the `LogDao` data.

[Raft]: https://raft.github.io

## Notes

  * Force a join in otternet: http://localhost:8080/control/join/http%3A%2F%2F127.0.0.1%3A8081
