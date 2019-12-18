from raft.messaging import Message, AppendEntriesSucceeded


def test_message_conversion():
    message = Message(server_no=0, term=0, content=AppendEntriesSucceeded(1))
    assert Message.from_bytes(bytes(message)) == message
