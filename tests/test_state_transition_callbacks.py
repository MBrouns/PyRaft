from raft.messaging import RequestVote
from raft.server import State


def test_become_leader(no_network_raft_leader, filled_log):
    no_network_raft_leader.log = filled_log
    no_network_raft_leader.become_leader()

    assert no_network_raft_leader.match_index == [-1, -1, -1]
    assert no_network_raft_leader.next_index == [6, 6, 6]
    assert no_network_raft_leader.state == State.LEADER


def test_become_candidate(no_network_raft_follower):
    no_network_raft_follower.become_candidate()
    no_network_raft_follower.voted_for = no_network_raft_follower.server_no
    no_network_raft_follower.votes_received = {no_network_raft_follower.server_no}
    assert no_network_raft_follower.term == 1

    for i in range(no_network_raft_follower.num_servers - 1):
        request_vote = no_network_raft_follower.outbox.get()
        assert (
            request_vote == "reset_election_timeout"
            or request_vote.content
            == RequestVote(term=1, candidate_log_len=0, last_log_term=0)
        )
