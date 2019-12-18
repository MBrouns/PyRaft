from unittest import mock


def test_handle_vote_granted_same_voter(no_network_raft_follower):
    no_network_raft_follower.handle_vote_granted(3)
    no_network_raft_follower.handle_vote_granted(3)
    assert len(no_network_raft_follower.received_votes) == 1


def test_handle_vote_granted_no_majority(no_network_raft_follower):
    with mock.patch.object(no_network_raft_follower, 'become_leader') as mocked_method:
        no_network_raft_follower.handle_vote_granted(3)
        assert not mocked_method.called


def test_handle_vote_granted_majority(no_network_raft_follower):
    no_network_raft_follower.received_votes = {2}
    with mock.patch.object(no_network_raft_follower, 'become_leader') as mocked_method:
        no_network_raft_follower.handle_vote_granted(3)
        assert mocked_method.called
