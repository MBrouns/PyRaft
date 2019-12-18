from raft.log import LogEntry
from raft.messaging import Message
from raft.server import LogAlreadyUpToDateException


def replicate(leader, follower):
    leader_response_msg = Message(
        server_no=leader.server_no,
        term=leader.term,
        content=leader.send_append_entries(follower.server_no),
    )

    while leader_response_msg is not None:
        follower_reply = Message(
            server_no=follower.server_no,
            term=follower.term,
            content=follower.handle_message(leader_response_msg),
        )
        leader_response = leader.handle_message(follower_reply)
        if leader_response is None:
            try:
                leader_response = leader.send_append_entries(follower.server_no)
            except LogAlreadyUpToDateException:
                break

        leader_response_msg = Message(
            server_no=leader.server_no, term=leader.term, content=leader_response
        )


def test_replicate_to_empty_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert len(no_network_raft_leader_with_log.log) == len(no_network_raft_follower.log)
    for i in range(len(no_network_raft_follower.log)):
        assert no_network_raft_leader_with_log.log[i] == no_network_raft_follower.log[i]


def test_replicate_to_complete_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    no_network_raft_follower.log._log = list(no_network_raft_leader_with_log.log._log)
    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert len(no_network_raft_leader_with_log.log) == len(no_network_raft_follower.log)
    for i in range(len(no_network_raft_follower.log)):
        assert no_network_raft_leader_with_log.log[i] == no_network_raft_follower.log[i]


def test_replicate_to_partial_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    """Test whether log replication succesfully completes when starting with a partial log"""
    no_network_raft_follower.log._log = [
        no_network_raft_leader_with_log.log[0],
        no_network_raft_leader_with_log.log[1],
    ]

    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert len(no_network_raft_leader_with_log.log) == len(no_network_raft_follower.log)
    for i in range(len(no_network_raft_follower.log)):
        assert no_network_raft_leader_with_log.log[i] == no_network_raft_follower.log[i]


def test_replicate_to_diverging_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    """Tests whether log replication recovers after a split brain"""
    no_network_raft_follower.log.append(log_index=0, prev_log_term=0, entry=LogEntry(term=0, msg=0))
    no_network_raft_follower.log.append(log_index=1, prev_log_term=0, entry=LogEntry(term=0, msg=1))
    no_network_raft_follower.log.append(log_index=2, prev_log_term=0, entry=LogEntry(term=0, msg=2))
    no_network_raft_follower.log.append(log_index=3, prev_log_term=0, entry=LogEntry(term=0, msg=3))
    no_network_raft_follower.log.append(log_index=4, prev_log_term=0, entry=LogEntry(term=0, msg=4))
    no_network_raft_follower.log.append(log_index=5, prev_log_term=0, entry=LogEntry(term=0, msg=5))

    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert len(no_network_raft_leader_with_log.log) == len(no_network_raft_follower.log)
    for i in range(len(no_network_raft_follower.log)):
        assert no_network_raft_leader_with_log.log[i] == no_network_raft_follower.log[i]
