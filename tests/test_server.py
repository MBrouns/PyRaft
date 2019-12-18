def test_increment_term(no_network_raft_follower):
    no_network_raft_follower.voted_for = 1
    no_network_raft_follower.term = 1
    assert no_network_raft_follower.voted_for is None
