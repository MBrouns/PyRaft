import pytest
from raft import config
from raft.network import SockBackend
from raft.server import RaftServer


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
        self.state = 'leader'


class MockFollower:
    def __init__(self):
        self.state = 'follower'


@pytest.fixture
def no_network_raft_follower():
    return RaftServer(server_no=0, num_servers=1, net=MockBackend(0, config.SERVERS), state_machine=MockFollower())


@pytest.fixture
def no_network_raft_leader():
    return RaftServer(server_no=0, num_servers=1, net=MockBackend(0, config.SERVERS), state_machine=MockLeader())
