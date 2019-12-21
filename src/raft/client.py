import uuid
from socket import socket, AF_INET, SOCK_STREAM, timeout

from raft import config
from raft.messaging import (
    SetValue,
    send_message,
    Message,
    Command,
    recv_message,
    Result,
    NotTheLeader,
    NoOp,
    GetValue,
    DelValue,
    ClientDisconnected)


class NoConnectionError(Exception):
    pass


class DistDict:
    def __init__(self, server_config, timeout=config.CLIENT_TIMEOUT):
        self.server_config = server_config
        self.client_id = uuid.uuid1()
        self.timeout = timeout
        self._cached_sock = None
        self._leader_no = None

    def _connect_server(self, server_no):
        s = socket(AF_INET, SOCK_STREAM)
        s.settimeout(1)
        s.connect(self.server_config[server_no])
        s.settimeout(self.timeout)
        return s

    def _request_id(self):
        return uuid.uuid1()

    def _find_leader(self):
        for server_no in self.server_config:
            try:
                s = self._connect_server(server_no)
            except OSError:
                continue
            else:
                no_op_msg = Message(
                    sender=self.client_id,
                    recipient=server_no,
                    term=None,
                    content=Command(operation=NoOp(request_id=self._request_id())),
                )
                send_message(s, bytes(no_op_msg))
                response = Message.from_bytes(recv_message(s))

                if isinstance(response.content, Result):
                    return s, response.sender
                elif isinstance(response.content, NotTheLeader):
                    if response.content.leader_id is None:
                        raise NoConnectionError("server didn't know who the leader was")
                    return (
                        self._connect_server(response.content.leader_id),
                        response.content.leader_id,
                    )
        raise NoConnectionError("couldn't connect to any machine in the cluster")

    def send(self, content):
        try:
            message = Message(
                sender=self.client_id,
                recipient=self._leader_no,
                term=None,
                content=content,
            )
            send_message(self._cached_sock, bytes(message))
            resp = Message.from_bytes(recv_message(self._cached_sock))

            print("got response", resp)
            if isinstance(resp.content, NotTheLeader):
                raise ConnectionRefusedError(f"tried to connect to server {self._leader_no} but it was not the leader")

        except (AttributeError, ConnectionRefusedError, ClientDisconnected, timeout):
            print(f"was either connected to a server that wasn't the leader, or got a timeout")
            self._cached_sock, self._leader_no = self._find_leader()
            return self.send(content)
        else:
            return resp.content.content

    def __setitem__(self, key, value):
        return self.send(
            Command(SetValue(request_id=self._request_id(), key=key, value=value))
        )

    def __getitem__(self, item):
        return self.send(Command(GetValue(request_id=self._request_id(), key=item)))

    def __delitem__(self, key):
        return self.send(Command(DelValue(request_id=self._request_id(), key=key)))
