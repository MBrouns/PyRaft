import logging
import click
from raft import config
from raft.controller import RaftController
from raft.log import LogEntry
from raft.network import SockBackend
from raft.server import RaftServer


def setup_logger(verbose, log_file_path):
    log_level = logging.DEBUG if verbose else logging.INFO
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(log_file_path)
    c_handler.setLevel(log_level)
    f_handler.setLevel(log_level)

    format = "%(asctime)s - %(relativeCreated)6d %(threadName)s  - %(name)s - %(levelname)s - %(message)s"
    c_format = logging.Formatter(format)
    f_format = logging.Formatter(format)
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    logging.basicConfig(level=log_level, handlers=[c_handler, f_handler])

@click.group()
@click.option('-v', '--verbose', is_flag=True)
@click.option('-l', '--log-file-path', default='raft.log')
def main(verbose, log_file_path):
    setup_logger(verbose, log_file_path)


class MockLeader:
    def __init__(self):
        self.state = State.LEADER


class MockFollower:
    def __init__(self):
        self.state = State.FOLLOWER


@main.command()
def start():
    pass
    # r0 = RaftController(0, RaftServer(server_no=0, num_servers=5), SockBackend(0, config.SERVERS))
    # r1 = RaftController(1, RaftServer(server_no=1, num_servers=5), SockBackend(1, config.SERVERS))
    # r2 = RaftController(2, RaftServer(server_no=2, num_servers=5), SockBackend(2, config.SERVERS))
    # r3 = RaftController(3, RaftServer(server_no=3, num_servers=5), SockBackend(3, config.SERVERS))
    # r4 = RaftController(4, RaftServer(server_no=4, num_servers=5), SockBackend(4, config.SERVERS))
    #
    #
    # r0._machine.become_leader()
    # print('hi')

if __name__ == "__main__":
    setup_logger(True, 'raft.log')
    r0 = RaftController(0, RaftServer(server_no=0, num_servers=5), SockBackend(0, config.SERVERS))
    r1 = RaftController(1, RaftServer(server_no=1, num_servers=5), SockBackend(1, config.SERVERS))
    r2 = RaftController(2, RaftServer(server_no=2, num_servers=5), SockBackend(2, config.SERVERS))
    r3 = RaftController(3, RaftServer(server_no=3, num_servers=5), SockBackend(3, config.SERVERS))
    r4 = RaftController(4, RaftServer(server_no=4, num_servers=5), SockBackend(4, config.SERVERS))

    def start():
        r0.start()
        r1.start()
        r2.start()
        r3.start()
        r4.start()

    def create_entries():
        r0._machine.log.append(0, 0, LogEntry(0, 'hello world'))
        r0._machine.log.append(1, 0, LogEntry(0, 'hello world'))

