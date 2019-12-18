import pytest
from raft import config
from raft.log import Log, LogEntry
from raft.network import SockBackend
from raft.server import RaftServer
from raft.state_machine import State


class MockBackend:
    def __init__(self, server_no, server_config):
        self.server_no = server_no

    def run_server(self):
        pass

    def send(self, server_no, msg):
        pass

    def recv(self):
        pass


class MockLeader:
    def __init__(self):
        self.state = State.LEADER


class MockFollower:
    def __init__(self):
        self.state = State.FOLLOWER


@pytest.fixture
def no_network_raft_follower():
    return RaftServer(
        server_no=0,
        num_servers=3,
        net=MockBackend(0, config.SERVERS),
        state_machine=MockFollower(),
    )


@pytest.fixture
def no_network_raft_leader():
    return RaftServer(
        server_no=0,
        num_servers=3,
        net=MockBackend(0, config.SERVERS),
        state_machine=MockLeader(),
    )


@pytest.fixture
def filled_log():
    log = Log()
    term_0_entry = LogEntry(term=0, msg=42)
    term_1_entry = LogEntry(term=1, msg=42)

    log.append(log_index=0, prev_log_term=0, entry=term_0_entry)
    log.append(log_index=1, prev_log_term=0, entry=term_0_entry)
    log.append(log_index=2, prev_log_term=0, entry=term_0_entry)
    log.append(log_index=3, prev_log_term=0, entry=term_1_entry)
    log.append(log_index=4, prev_log_term=1, entry=term_1_entry)
    log.append(log_index=5, prev_log_term=1, entry=term_1_entry)

    assert len(log) == 6
    return log


@pytest.fixture
def no_network_raft_leader_with_log(no_network_raft_leader, filled_log):
    no_network_raft_leader.log = filled_log
    no_network_raft_leader.on_transition_leader()
    return no_network_raft_leader
