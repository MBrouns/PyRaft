from raft.log import LogEntry
from raft.messaging import AppendEntries


def test_send_append_entriers_already_replicated(no_network_raft_leader_with_log):
    receiver_server_no = 1
    no_network_raft_leader_with_log.next_index[receiver_server_no] += 1
    result = no_network_raft_leader_with_log._append_entries_msg(receiver_server_no)
    assert result == AppendEntries(log_index=6, prev_log_term=1, entry=None, leader_commit=0)


def test_send_append_entries(no_network_raft_leader_with_log):
    receiver_server_no = 1
    result = no_network_raft_leader_with_log._append_entries_msg(
        receiver_server_no
    )
    assert result == AppendEntries(
        log_index=5, prev_log_term=1, entry=LogEntry(term=1, msg=5), leader_commit=0
    )
