*In process.*

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
