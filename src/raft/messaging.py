import pickle
import logging
from typing import NamedTuple

logger = logging.getLogger(__name__)
HEADER_LENGTH = 8
HEADER_BYTEORDER = "big"


class ClientDisconnected(Exception):
    pass


AppendEntries = NamedTuple(
    "AppendEntries", log_index=int, prev_log_term=int, entry=str, leader_commit=int
)
AppendEntriesSucceeded = NamedTuple("AppendEntriesSucceeded", replicated_index=int)
AppendEntriesFailed = NamedTuple("AppendEntriesFailed", reason=Exception)
InvalidTerm = NamedTuple("InvalidTerm")
RequestVote = NamedTuple(
    "RequestVote", term=int, candidate_log_len=int, last_log_term=int
)
VoteGranted = NamedTuple("VoteGranted")
VoteDenied = NamedTuple("VoteDenied", reason=str)

# Client request messages

Command = NamedTuple("Command", operation=any)  # one of the below
SetValue = NamedTuple("SetValue", request_id=str, key=str, value=str)
GetValue = NamedTuple("GetValue", request_id=str, key=str)
DelValue = NamedTuple("DelValue", request_id=str, key=str)
NoOp = NamedTuple("NoOp", request_id=str)

Result = NamedTuple("Result", content=str)
NotTheLeader = NamedTuple("NotTheLeader", leader_id=int)


class Message:
    ALLOWED_MESSAGES = (
        AppendEntries,
        AppendEntriesSucceeded,
        AppendEntriesFailed,
        InvalidTerm,
        RequestVote,
        VoteGranted,
        VoteDenied,
        Command,
        Result,
        NotTheLeader,
    )

    def __init__(self, sender, term, recipient, content):
        if not isinstance(content, self.ALLOWED_MESSAGES):
            raise ValueError(
                f"expected message to be in {self.ALLOWED_MESSAGES}, got {type(content)}"
            )
        self.sender = sender
        self.term = term
        self.recipient = recipient
        self.content = content

    def __bytes__(self):
        return pickle.dumps(self.__dict__)

    @classmethod
    def from_bytes(cls, bytes):
        return cls(**pickle.loads(bytes))

    def __eq__(self, other):
        if not isinstance(other, Message):
            return NotImplemented

        return self.__dict__ == other.__dict__

    def __str__(self):
        return f"Message from {self.sender} to {self.recipient} on term {self.term} containing {self.content}"


def send_message(sock, msg):
    assert isinstance(msg, bytes)
    header = len(msg).to_bytes(HEADER_LENGTH, byteorder=HEADER_BYTEORDER)
    logger.debug(f"sending header")
    sock.send(header)
    logger.debug(f"sending {len(msg)} bytes of data")
    sock.sendall(msg)


def recv_message(sock):
    msg_length = _recv_size(sock)
    logger.debug(f"preparing to receive {msg_length} bytes of data")
    msg = _recv(sock, msg_length)
    return msg


def _recv(sock, size):
    data = b""
    while len(data) < size:
        msg = sock.recv(size - len(data))
        logger.debug(f"received {len(msg)}/{size} bytes of data")
        if not msg:
            raise ClientDisconnected()
        data += msg
    return data


def _recv_size(sock):
    data = _recv(sock, HEADER_LENGTH)
    msg_length = int.from_bytes(data, byteorder=HEADER_BYTEORDER)

    logger.debug(f"expecting a message of size {msg_length}")
    return msg_length
