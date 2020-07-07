# PyRaft
An implementation of the Raft Distributed Concensus Protocol in Python

**Note: This is not production ready software**

# Why?
This project was built during the [week long rafting-trip course](http://dabeaz.com/raft.html) offered by David Beazley 

# PyRaft
An implementation of the Raft Distributed Concensus Protocol in Python

**Note: This is not production ready software**

This project was built during the [week long rafting-trip course](http://dabeaz.com/raft.html) offered by David Beazley. It contains a basic implementation of the Raft Distributed Concensus Protocol including:

- Socket communications between servers
- Log replication
- Leader election
- Client request idempotency
- A key-value state machine
- A basic key-value client

Some things that are missing:

- Unit tests on the networking layer
- Integration tests
- Snapshotting
- Cluster configuration changes


## Usage
servers can be started using a provided CLI utility:
```
raft start --server 0
raft start --server 1
raft start --server 2
raft start --server 3
raft start --server 4
```
These servers will use the configuration as specified in `src.raft.config`. Once started, the servers will log their status to the console

```
---- Server: 1 ----
healthy: True
state: State.FOLLOWER
term: 277
voted for: 4
commit index: 71
election timeout: 0/43
------ Log ------
 *  LogEntry(term=277, msg=SetValue(request_id=UUID('47213396-24d1-11ea-83a2-f2189812bf51'), key='a', value='b'))
 *  LogEntry(term=277, msg=NoOp(request_id=0))
 *  LogEntry(term=276, msg=NoOp(request_id=0))
 *  LogEntry(term=273, msg=NoOp(request_id=0))
 *  LogEntry(term=270, msg=NoOp(request_id=0))
 *  LogEntry(term=267, msg=NoOp(request_id=0))
 *  LogEntry(term=264, msg=NoOp(request_id=0))
 *  LogEntry(term=256, msg=NoOp(request_id=0))
 *  LogEntry(term=255, msg=NoOp(request_id=0))
 *  LogEntry(term=254, msg=NoOp(request_id=0))
```

the `healthy` flag indicates whether all the servers controller threads are active. Unhealthy servers can be caused by crashes in the networking layer. If that happens, the best course of action would be to restart the offending server

the `election timeout` is measured in heartbeats

a `*` before the log entry indicates the entry has been committed and applied to the state machine. 

## Client
A client can be initialized as follows:

```
>>> rom raft.client import DistDict
>>> from raft.config import SERVERS
>>> client = DistDict(SERVERS)

>>> client['a'] = 'b'
>>> client['a']
b
>>> del client['a']
>>> client['a']
None
```

## Config
All config is done through the `src.raft.config` file. It contains properties such as the timeout for heartbeats, and the range of heartbeats before an election timeout is called. This should probably be moved into environment variables at some point.

## Architecture
The implementation heavily relies on threads for concurrency. The raft "business logic" however runs in a single thread. 

The raft business logic is implemented in the `src.raft.server.RaftServer` class which contains the basic `leader`, `candidate`, `follower` state machine as well as several public methods to handle events. These include:

- `handle_message`
- `handle_election_timeout`
- `handle_heartbeat`

These methods are called by the `src.raft.controller.RaftController` in its event loop. This event loop pops events such as messages and heartbeats off of an event queue.

The `RaftServer` also implements an `outbox` queue on which it can put events such as messages for other servers or clients, as well as events for the controller such as electiont imeout resets. 

It should be possible to replace the current thread-based implementation of the controller with an `async-io` implementation. This would require the outbox in the `RaftServer` to be injected though, rather than initialized inside the `RaftServer`s `__init__`. 

The `RaftController` relies on the `raft.network.SockBackend` to relay messages to clients and other servers. Messages are prefixed with a fixed-size, 8-bit header indicating the length of the message to follow. The Messages themselves are simply pickled objects of class `raft.messaging.Message`
