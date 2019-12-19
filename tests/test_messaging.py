from raft.messaging import Message, AppendEntriesSucceeded


def test_message_conversion():
    message = Message(sender=0, term=0, recipient=0, content=AppendEntriesSucceeded(1))
    assert Message.from_bytes(bytes(message)) == message
