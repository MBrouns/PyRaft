import queue
import threading
from socket import *
import logging

from raft.messaging import recv_message, send_message, Message


class SockBackend:
    def __init__(self, server_no, server_config):
        self.inbox = queue.Queue()
        self.host, self.port = server_config[server_no]
        self._server_no = server_no
        self._logger = logging.getLogger(f"RaftServer-{server_no}")
        self._server_config = server_config
        self._connections = [None for _ in range(len(server_config))]

    def start(self):
        threading.Thread(
            target=self._accept_clients,
            daemon=True,
            name=f"server_{self._server_no}_listener",
        ).start()

    def broadcast(self, msg):
        pass

    def send(self, server_no, msg):
        if server_no == self._server_no:
            raise ValueError(f"server {server_no} tried to send message to self")

        s = self._connections[server_no]
        try:
            send_message(s, bytes(msg))
            self._logger.info(
                f"sending a message to {server_no} using a cached connection"
            )
        except (AttributeError, ConnectionRefusedError):
            self._logger.info(
                f"no cached connection for server {server_no}, connecting to {self._server_config[server_no]}"
            )
            s = socket(AF_INET, SOCK_STREAM)
            s.connect(self._server_config[server_no])
            self._connections[server_no] = s
            send_message(s, bytes(msg))

    def recv(self):
        return self.inbox.get()

    def _accept_clients(self):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind((self.host, self.port))
        sock.listen(1)
        self._logger.info(
            f"Server {self._server_no} running at {self.host}:{self.port}"
        )
        while True:
            client, addr = sock.accept()
            self._logger.info(f"client connection received from server at {addr}")
            t = threading.Thread(
                target=self._handle_client,
                args=(client,),
                daemon=True,
                name=f"server_{self._server_no}_{addr}_handler",
            )
            t.start()

    def _handle_client(self, client):
        while True:
            raw_msg = recv_message(client)
            msg = Message.from_bytes(raw_msg)
            self._logger.info(
                f"received message of {len(raw_msg)} bytes from server {msg.sender}"
            )
            self.inbox.put(msg)
