import pickle
import sys
from socket import *
import logging

logger = logging.getLogger(__name__)
HEADER_LENGTH = 8
HEADER_BYTEORDER = 'big'


class ClientDisconnected(Exception):
    pass


class Message:
    def __init__(self, server_no, message):
        self.server_no = server_no
        self.message = message

    def __bytes__(self):
        return pickle.dumps({'server_no': self.server_no, 'message': self.message})

    @classmethod
    def from_bytes(cls, bytes):
        return cls(**pickle.loads(bytes))


def send_message(sock, msg):
    assert isinstance(msg, bytes)
    header = len(msg).to_bytes(HEADER_LENGTH, byteorder=HEADER_BYTEORDER)
    logger.debug(f'sending header')
    sock.send(header)
    logger.debug(f'sending {len(msg)} bytes of data')
    sock.sendall(msg)


def recv_message(sock):
    msg_length = _recv_size(sock)
    logger.debug(f'preparing to receive {msg_length} bytes of data')
    msg = _recv(sock, msg_length)
    return msg


def _recv(sock, size):
    data = b''
    while len(data) < size:
        msg = sock.recv(size - len(data))
        logger.debug(f'received {len(msg)}/{size} bytes of data')
        if not msg:
            raise ClientDisconnected()
        data += msg
    return data


def _recv_size(sock):
    data = _recv(sock, HEADER_LENGTH)
    msg_length = int.from_bytes(data, byteorder=HEADER_BYTEORDER)

    logger.debug(f'expecting a message of size {msg_length}')
    return msg_length


