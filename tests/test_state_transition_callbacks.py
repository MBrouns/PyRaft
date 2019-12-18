from raft.state_machine import State


def test_become_leader(no_network_raft_leader, filled_log):
    no_network_raft_leader.log = filled_log
    no_network_raft_leader.become_leader()

    assert no_network_raft_leader.match_index == [0, 0, 0]
    assert no_network_raft_leader.next_index == [5, 5, 5]
    assert no_network_raft_leader.state == State.LEADER

