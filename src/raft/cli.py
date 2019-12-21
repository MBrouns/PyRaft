import logging
import os
import time

import click
from raft import config
from raft.client import DistDict
from raft.controller import RaftController
from raft.network import SockBackend
from raft.server import RaftServer
from raft.state_machine import KVStateMachine


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

    logging.basicConfig(level=log_level, handlers=[f_handler])


@click.group()
@click.option("-v", "--verbose", is_flag=True)
@click.option("-l", "--log-file-path", default="raft.log")
def main(verbose, log_file_path):
    pass

@main.command()
def client():
    client = DistDict(config.SERVERS)

@main.command()
@click.option("-s", "--server", type=int)
# @click.option("c", "--config-path", type=int)
def start(server):
    setup_logger(False, f'raft-{server}.log')

    controller = RaftController(
        server,
        RaftServer(
            server_no=server, num_servers=5, persist=True, state_machine=KVStateMachine(0)
        ),
        SockBackend(server, config.SERVERS),
    )
    controller.start()

    while controller.healthy:
        time.sleep(0.5)
        os.system('clear')
        print(f"---- Server: {server} ----")
        print(f"healthy: {controller.healthy()}")
        print(f"state: {controller._machine.state}")
        print(f"term: {controller._machine.term}")
        print(f"voted for: {controller._machine.voted_for}")
        print(f"commit index: {controller._machine.commit_index}")
        print(f"election timeout: {controller.hb_since_election_timeout}/{controller.hb_for_election_timeout}")
        print(f"------ Log ------ ")
        for idx, entry in reversed(list(enumerate(controller._machine.log._log[-10::]))):
            c_index = controller._machine.commit_index
            log_index = idx - min(len(controller._machine.log._log[-10::]), 10) + len(controller._machine.log)
            prefix = "   " if log_index > c_index else " * "
            print(prefix, entry)


if __name__ == "__main__":
    main()
