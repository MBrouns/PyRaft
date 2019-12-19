from raft.log import LogNotCaughtUpError
from raft.messaging import AppendEntries


def test_handle_append_entries_failed_not_leader(no_network_raft_follower):
    resp = no_network_raft_follower._handle_append_entries_failed(
        1, LogNotCaughtUpError()
    )
    assert resp is None


def test_handle_append_entries_failed(no_network_raft_leader_with_log):
    match_index_before = list(no_network_raft_leader_with_log.match_index)
    next_index_before = list(no_network_raft_leader_with_log.next_index)

    sender_server_no = 1
    resp = no_network_raft_leader_with_log._handle_append_entries_failed(sender_server_no, LogNotCaughtUpError())
    assert resp == AppendEntries(
        log_index=next_index_before[sender_server_no] - 1,
        prev_log_term=no_network_raft_leader_with_log.log[next_index_before[sender_server_no] - 1].term,
        entry=no_network_raft_leader_with_log.log[next_index_before[sender_server_no] - 1],
        leader_commit=no_network_raft_leader_with_log.commit_index,
    )

    assert no_network_raft_leader_with_log.match_index == match_index_before
    assert no_network_raft_leader_with_log.next_index[0] == next_index_before[0]
    assert no_network_raft_leader_with_log.next_index[2] == next_index_before[2]
    assert no_network_raft_leader_with_log.next_index[1] == next_index_before[1] - 1
