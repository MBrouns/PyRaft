import pytest
from raft.log import LogEntry
from raft.messaging import AppendEntries
from raft.server import LogAlreadyUpToDateException


def test_send_append_entriers_already_replicated(no_network_raft_leader_with_log):
    receiver_server_no = 1
    no_network_raft_leader_with_log.next_index[receiver_server_no] += 1
    with pytest.raises(LogAlreadyUpToDateException):
        no_network_raft_leader_with_log.send_append_entries(receiver_server_no)


def test_send_append_entries(no_network_raft_leader_with_log):
    receiver_server_no = 1
    assert no_network_raft_leader_with_log.send_append_entries(
        receiver_server_no
    ) == AppendEntries(log_index=5, prev_log_term=1, entry=LogEntry(term=1, msg=5), leader_commit=0)
