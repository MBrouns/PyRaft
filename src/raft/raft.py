import logging
import time

from raft import config
from raft.messaging import Message
from raft.network import SockBackend


class RaftServer:
    def __init__(self, server_no, net):
        self.server_no = server_no
        self.logger = logging.getLogger(f'RaftServer-{server_no}')

        self.net = net
        self.net.run_server()

    def recv(self):
        return self.net.inbox.get()

    def send(self, server_no, msg):
        self.net.send(server_no, msg)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(relativeCreated)6d %(threadName)s  - %(name)s - %(levelname)s - %(message)s'
    )
    h0 = RaftServer(0, SockBackend(0, config.SERVERS))
    h1 = RaftServer(1, SockBackend(1, config.SERVERS))

    time.sleep(1)
    h0.send(1, Message(0, 'hello world'))
    h0.send(1, Message(0, 'another world'))
    print(h1.recv())
