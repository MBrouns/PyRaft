# Raft


## Leader election

## Log replication

## Safety


## Node states

Nodes are in one of three states: 
- Follower
- Candidate
- Leader


## General
Decouple raft algorithm from socket server

How to build the state machine? Different classes for each state?

Forget about:
 - Membership changes
 - Log compaction
 
 
## Approach 1: Start with fixed leader and build log replication
`append_entries` is easy to build in isolation as long as it is properly decoupled


## Approach 2: Start with leader election with empty logs
cons:
 - the safety methods of the leader election depend on the log
 - still need to build appendlog methods for heartbeats and stuff
 
## unclear parts:
- configuration changes
- log compaction


- Client interaction:

> However, as described so far Raft can exe- cute a command multiple times: for example, 
if the leader crashes after committing the log entry but before responding to the client, 
the client will retry the command with a new leader, causing it to be executed a second time. 
The solution is for clients to assign unique serial numbers to every command. 
Then, the state machine tracks the latest serial number processed for each client, 
along with the as- sociated response. If it receives a command whose serial number has 
already been executed, it responds immediately without re-executing the request.

That means clients need to send monotonic increasing serial numbers which are committed to the logs?


