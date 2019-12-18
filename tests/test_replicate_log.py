from raft.messaging import Message, AppendEntriesFailed

def test_replicate_log(no_network_raft_leader_with_log, no_network_raft_follower):
    receiver_server_no = 1
    no_network_raft_leader_with_log.next_index[receiver_server_no] -= 1

    leader_response_msg = Message(
        server_no=no_network_raft_leader_with_log.server_no,
        term=no_network_raft_leader_with_log.term,
        content=no_network_raft_leader_with_log.send_append_entries(receiver_server_no)
    )

    while leader_response_msg is not None:
        follower_reply = Message(
            server_no=no_network_raft_follower.server_no,
            term=no_network_raft_follower.term,
            content=no_network_raft_follower.handle_message(leader_response_msg)
        )
        leader_response = no_network_raft_leader_with_log.handle_message(follower_reply)
        if leader_response is None:
            leader_response = no_network_raft_leader_with_log.send_append_entries(receiver_server_no)
            if leader_response is None:
                break

        leader_response_msg = Message(
            server_no=no_network_raft_leader_with_log.server_no,
            term=no_network_raft_leader_with_log.term,
            content=leader_response
        )

    assert len(no_network_raft_leader_with_log.log) == len(no_network_raft_follower.log)
    for i in range(len(no_network_raft_follower.log)):
        assert no_network_raft_leader_with_log.log[i] == no_network_raft_follower.log[i]
