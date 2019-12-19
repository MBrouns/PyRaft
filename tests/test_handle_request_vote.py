from raft.messaging import RequestVote, VoteDenied, VoteGranted


def test_handle_request_vote_stale_term(no_network_raft_follower):
    no_network_raft_follower.term = 1
    resp = no_network_raft_follower._handle_request_vote(
        term=0, candidate_id=1, candidate_log_len=1, last_log_term=1
    )

    assert isinstance(resp, VoteDenied)
    assert resp.reason == "Vote came from server on term 0 while own term was 1"


def test_handle_request_vote_already_voted(no_network_raft_follower):
    no_network_raft_follower.voted_for = 2
    resp = no_network_raft_follower._handle_request_vote(
        term=0, candidate_id=1, candidate_log_len=1, last_log_term=1
    )

    assert isinstance(resp, VoteDenied)
    assert resp.reason == "already voted for other candidate: 2"


def test_handle_request_vote_already_voted_same_candidate(no_network_raft_follower):
    no_network_raft_follower.voted_for = 1
    resp = no_network_raft_follower._handle_request_vote(
        term=0, candidate_id=1, candidate_log_len=1, last_log_term=1
    )

    assert isinstance(resp, VoteGranted)


def test_handle_request_vote_stale_log_len(no_network_raft_follower, filled_log):
    no_network_raft_follower.log = filled_log
    resp = no_network_raft_follower._handle_request_vote(
        term=0, candidate_id=1, candidate_log_len=1, last_log_term=1
    )

    assert isinstance(resp, VoteDenied)
    assert (
        resp.reason
        == f"candidate log was size 1 while own log length was {len(filled_log)}"
    )


def test_handle_request_vote_stale_log_term(no_network_raft_follower, filled_log):
    no_network_raft_follower.log = filled_log
    resp = no_network_raft_follower._handle_request_vote(
        term=0, candidate_id=1, candidate_log_len=len(filled_log), last_log_term=0
    )

    assert isinstance(resp, VoteDenied)
    assert (
        resp.reason
        == f"candidate log was on term 0 while own log length was {filled_log.last_term}"
    )


def test_handle_request_vote_success(no_network_raft_follower):
    resp = no_network_raft_follower._handle_request_vote(
        term=0, candidate_id=1, candidate_log_len=1, last_log_term=1
    )
    assert isinstance(resp, VoteGranted)
