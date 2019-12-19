from raft.log import LogNotCaughtUpError, LogDifferentTermError
from raft.messaging import AppendEntriesSucceeded, AppendEntriesFailed


def test_append_entries_sets_leader(no_network_raft_follower, log_entry):
    result = no_network_raft_follower._handle_append_entries(
        leader_id=4, log_index=0, prev_log_term=0, entry=log_entry, leader_commit=0
    )
    assert isinstance(result, AppendEntriesSucceeded)
    assert no_network_raft_follower.leader_id == 4


def test_append_entries_future(no_network_raft_follower, log_entry):
    """AppendEntries receiver implementation bullet 2"""
    result = no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=1, prev_log_term=0, entry=log_entry, leader_commit=0
    )
    assert isinstance(result, AppendEntriesFailed)
    assert isinstance(result.reason, LogNotCaughtUpError)


def test_append_entries_split_brain(no_network_raft_follower, log_entry):
    """AppendEntries receiver implementation bullet 2"""

    result = no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=0, prev_log_term=0, entry=log_entry, leader_commit=0
    )
    assert isinstance(result, AppendEntriesSucceeded)
    assert result.replicated_index == 0

    result = no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=1, prev_log_term=1, entry=log_entry, leader_commit=0
    )
    assert isinstance(result, AppendEntriesFailed)
    assert isinstance(result.reason, LogDifferentTermError)


def test_append_entries_normal(no_network_raft_follower, log_entry):
    result = no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=0, prev_log_term=0, entry=log_entry, leader_commit=0
    )
    assert isinstance(result, AppendEntriesSucceeded)



def test_append_entries_None(no_network_raft_follower, log_entry):
    result = no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=0, prev_log_term=0, entry=log_entry, leader_commit=0
    )
    result = no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=1, prev_log_term=0, entry=None, leader_commit=0
    )
    assert isinstance(result, AppendEntriesSucceeded)
    assert result.replicated_index == 0


def test_append_entries_sets_commit_index(no_network_raft_follower, log_entry):
    result = no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=0, prev_log_term=0, entry=log_entry, leader_commit=4
    )
    assert isinstance(result, AppendEntriesSucceeded)
    assert no_network_raft_follower.commit_index == 0

    result = no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=1, prev_log_term=0, entry=log_entry, leader_commit=4
    )
    assert isinstance(result, AppendEntriesSucceeded)
    assert no_network_raft_follower.commit_index == 1


def test_append_entries_sets_commit_index_min(no_network_raft_follower, log_entry):
    no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=0, prev_log_term=0, entry=log_entry, leader_commit=0
    )
    no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=1, prev_log_term=0, entry=log_entry, leader_commit=0
    )
    no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=2, prev_log_term=0, entry=log_entry, leader_commit=0
    )

    result = no_network_raft_follower._handle_append_entries(
        leader_id=0, log_index=1, prev_log_term=0, entry=log_entry, leader_commit=4
    )
    assert isinstance(result, AppendEntriesSucceeded)
    assert no_network_raft_follower.commit_index == 1
