def test_handle_append_entries_failed_not_leader(no_network_raft_follower):
    resp = no_network_raft_follower.handle_append_entries_succeeded(
        other_server_no=1, replicated_index=1
    )
    assert resp is None


def test_handle_append_entries_succeeded(no_network_raft_leader_with_log):
    sender_server_no = 1
    resp = no_network_raft_leader_with_log.handle_append_entries_succeeded(
        other_server_no=sender_server_no, replicated_index=1
    )
    assert resp is None

    assert no_network_raft_leader_with_log.match_index == [0, 1, 0]
    assert no_network_raft_leader_with_log.next_index == [5, 2, 5]
