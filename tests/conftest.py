from unittest import mock

import pytest
from raft import config
from raft.controller import RaftController
from raft.log import Log, LogEntry
from raft.network import SockBackend
from raft.server import RaftServer, State
from raft.state_machine import LoggerStateMachine


class MockBackend:
    def __init__(self, server_no, server_config):
        self.server_no = server_no

    def run_server(self):
        pass

    def send(self, server_no, msg):
        pass

    def recv(self):
        pass


@pytest.fixture
def raft_controller():
    return RaftController()


@pytest.fixture
def no_network_raft_follower():
    return RaftServer(
        server_no=1,
        num_servers=3,
        state_machine=LoggerStateMachine(1)
    )


@pytest.fixture
def no_network_raft_leader():
    server = RaftServer(
        server_no=0,
        num_servers=3,
        state_machine=LoggerStateMachine(0)
    )
    server.become_leader()
    return server


@pytest.fixture
def log_entry():
    return LogEntry(term=0, msg=0)


@pytest.fixture
def filled_log():
    log = Log()

    log.append(log_index=0, prev_log_term=0, entry=LogEntry(term=0, msg=0))
    log.append(log_index=1, prev_log_term=0, entry=LogEntry(term=0, msg=1))
    log.append(log_index=2, prev_log_term=0, entry=LogEntry(term=0, msg=2))
    log.append(log_index=3, prev_log_term=0, entry=LogEntry(term=1, msg=3))
    log.append(log_index=4, prev_log_term=1, entry=LogEntry(term=1, msg=4))
    log.append(log_index=5, prev_log_term=1, entry=LogEntry(term=1, msg=5))

    assert len(log) == 6
    return log


@pytest.fixture
def raft_cluster():
    def impl(num_servers):
        return [
            RaftServer(server_no=i, num_servers=num_servers, state_machine=LoggerStateMachine(i))
            for i in range(num_servers)
        ]
    return impl

@pytest.fixture
def no_network_raft_leader_with_log(no_network_raft_leader, filled_log):
    no_network_raft_leader.log = filled_log
    with mock.patch.object(no_network_raft_leader, 'leader_log_append') as mocked:
        no_network_raft_leader.become_leader()
    return no_network_raft_leader
