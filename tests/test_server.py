from unittest import mock

from raft.log import Log


def test_increment_term(no_network_raft_follower):
    no_network_raft_follower.voted_for = 1
    no_network_raft_follower.term = 1
    assert no_network_raft_follower.voted_for is None


def test_increment_commit_index_calls_apply(no_network_raft_follower, log_entry):
    no_network_raft_follower.log = Log.from_entries([log_entry])
    with mock.patch.object(no_network_raft_follower._state_machine, 'apply') as mocked_apply:
        no_network_raft_follower.commit_index = 0
        assert mocked_apply.called


def test_increment_commit_index_doesnt_call_apply(no_network_raft_follower, log_entry):
    no_network_raft_follower.log = Log.from_entries([log_entry])
    no_network_raft_follower.last_applied = 1
    with mock.patch.object(no_network_raft_follower._state_machine, 'apply') as mocked_apply:
        no_network_raft_follower.commit_index = 0
        assert not mocked_apply.called
