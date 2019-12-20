import logging
import time
import uuid

import click
from raft import config
from raft.client import DistDict
from raft.controller import RaftController
from raft.log import LogEntry
from raft.messaging import SetValue
from raft.network import SockBackend
from raft.server import RaftServer
from raft.state_machine import LoggerStateMachine, KVStateMachine


def setup_logger(verbose, log_file_path):
    log_level = logging.DEBUG if verbose else logging.INFO
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(log_file_path, mode="w")
    c_handler.setLevel(log_level)
    f_handler.setLevel(log_level)

    format = "%(relativeCreated)6d %(threadName)30s - %(name)17s - %(levelname)s - %(message)s"
    c_format = logging.Formatter(format)
    f_format = logging.Formatter(format)
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    logging.basicConfig(level=log_level, handlers=[c_handler, f_handler])


@click.group()
@click.option("-v", "--verbose", is_flag=True)
@click.option("-l", "--log-file-path", default="raft.log")
def main(verbose, log_file_path):
    setup_logger(verbose, log_file_path)


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
    setup_logger(False, "raft.log")
    r0 = RaftController(
        0,
        RaftServer(server_no=0, num_servers=5, state_machine=KVStateMachine(0)),
        SockBackend(0, config.SERVERS),
    )
    r1 = RaftController(
        1,
        RaftServer(server_no=1, num_servers=5, state_machine=KVStateMachine(1)),
        SockBackend(1, config.SERVERS),
    )
    r2 = RaftController(
        2,
        RaftServer(server_no=2, num_servers=5, state_machine=KVStateMachine(2)),
        SockBackend(2, config.SERVERS),
    )
    r3 = RaftController(
        3,
        RaftServer(server_no=3, num_servers=5, state_machine=KVStateMachine(3)),
        SockBackend(3, config.SERVERS),
    )
    r4 = RaftController(
        4,
        RaftServer(server_no=4, num_servers=5, state_machine=KVStateMachine(4)),
        SockBackend(4, config.SERVERS),
    )

    def start():
        r0.start()
        r1.start()
        r2.start()
        r3.start()
        r4.start()

    def add_entry(controller, op):
        controller._machine.log.append(
            len(controller._machine.log),
            controller._machine.log.last_term,
            LogEntry(controller._machine.term, op),
        )

    def logs_same(log1, log2):
        if len(log1) != len(log2):
            raise ValueError(f"logs different length: {len(log1)} - {len(log2)}")
        for i in range(len(log2)):
            if log1[i] != log2[i]:
                raise ValueError(f"logs different in ind ex {i}")
        return True

    def all_logs_same():
        assert logs_same(r0._machine.log, r1._machine.log)
        assert logs_same(r0._machine.log, r2._machine.log)
        assert logs_same(r0._machine.log, r3._machine.log)
        assert logs_same(r0._machine.log, r4._machine.log)

    start()

    time.sleep(1)
    add_entry(r0, SetValue(request_id=uuid.uuid1(), key="a", value="1"))
    add_entry(r0, SetValue(request_id=uuid.uuid1(), key="b", value="2"))

    time.sleep(1)
    r0._machine.become_candidate()

    client = DistDict(config.SERVERS)
