import queue

from raft.log import LogEntry
from raft.messaging import Message


def replicate(leader, follower):
    """Mocks the messages transferring between leaders and followers to do a full cycle of log replication"""

    leader_response_msg = Message(
        sender=leader.server_no,
        term=leader.term,
        recipient=follower.server_no,
        content=leader._append_entries_msg(follower.server_no),
    )

    while leader_response_msg is not None:
        follower.handle_message(leader_response_msg)
        follower_reply = follower.outbox.get()

        leader.handle_message(follower_reply)
        try:
            leader_response_msg = leader.outbox.get(False)
        except queue.Empty:
            msg = leader._append_entries_msg(follower.server_no)
            if msg.entry is None:  # already replicated
                break
            leader.send(follower, msg)
            leader_response_msg = leader.outbox.get()



def logs_same(log1, log2):
    if len(log1) != len(log2):
        return False
    for i in range(len(log2)):
        if log1[i] != log2[i]:
            return False
    return True


def test_replicate_to_empty_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    """Tests whether log replication works when follower log is empty"""
    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert logs_same(no_network_raft_leader_with_log.log, no_network_raft_follower.log)


def test_replicate_to_complete_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    no_network_raft_follower.log._log = list(no_network_raft_leader_with_log.log._log)
    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert logs_same(no_network_raft_leader_with_log.log, no_network_raft_follower.log)


def test_replicate_to_partial_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    """Test whether log replication succesfully completes when starting with a partial log"""
    no_network_raft_follower.log._log = [
        no_network_raft_leader_with_log.log[0],
        no_network_raft_leader_with_log.log[1],
    ]

    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert logs_same(no_network_raft_leader_with_log.log, no_network_raft_follower.log)


def test_replicate_to_diverging_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    """Tests whether log replication recovers after a split brain"""
    no_network_raft_follower.log.append(
        log_index=0, prev_log_term=0, entry=LogEntry(term=0, msg=0)
    )
    no_network_raft_follower.log.append(
        log_index=1, prev_log_term=0, entry=LogEntry(term=0, msg=1)
    )
    no_network_raft_follower.log.append(
        log_index=2, prev_log_term=0, entry=LogEntry(term=0, msg=2)
    )
    no_network_raft_follower.log.append(
        log_index=3, prev_log_term=0, entry=LogEntry(term=0, msg=3)
    )
    no_network_raft_follower.log.append(
        log_index=4, prev_log_term=0, entry=LogEntry(term=0, msg=4)
    )
    no_network_raft_follower.log.append(
        log_index=5, prev_log_term=0, entry=LogEntry(term=0, msg=5)
    )

    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert logs_same(no_network_raft_leader_with_log.log, no_network_raft_follower.log)
