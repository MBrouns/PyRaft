import queue
import threading
from socket import *
import logging

from raft.messaging import recv_message, send_message, Message, ClientDisconnected


class SockBackend:
    def __init__(self, server_no, server_config):
        self.inbox = queue.Queue()
        self.host, self.port = server_config[server_no]
        self._server_no = server_no
        self._logger = logging.getLogger(f"RaftServer-{server_no}")
        self._server_config = server_config
        self._connections = {server_no: None for server_no in server_config.keys()}

    def start(self):
        threading.Thread(
            target=self._accept_clients,
            daemon=True,
            name=f"server_{self._server_no}_listener",
        ).start()

    def broadcast(self, msg):
        pass

    def send(self, msg):
        recipient = msg.recipient
        if recipient == self._server_no:
            raise ValueError(f"server {recipient} tried to send message to self")

        s = self._connections[recipient]
        try:
            send_message(s, bytes(msg))
            self._logger.debug(
                f"sending a message to {recipient} using a cached connection"
            )
        except (timeout, BrokenPipeError, ConnectionRefusedError):
            self._connections[recipient] = None  # retry next time
        except (AttributeError, ConnectionRefusedError):
            self._logger.debug(
                f"no cached connection for server {recipient}, connecting to {self._server_config[recipient]}"
            )
            s = socket(AF_INET, SOCK_STREAM)
            try:
                s.connect(self._server_config[recipient])
                s.settimeout(1)
            except ConnectionRefusedError:
                pass
            else:
                self._connections[recipient] = s
                send_message(s, bytes(msg))

    def recv(self):
        return self.inbox.get()

    def _accept_clients(self):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind((self.host, self.port))
        sock.listen(len(self._server_config))
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
            try:
                raw_msg = recv_message(client)
            except ClientDisconnected:
                return
            msg = Message.from_bytes(raw_msg)
            if not isinstance(msg.sender, int):
                # must be a client
                self._connections[msg.sender] = client

            self._logger.debug(
                f"received message of {len(raw_msg)} bytes from server {msg.sender}"
            )
            self.inbox.put(msg)
