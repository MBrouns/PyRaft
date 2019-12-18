from unittest import mock

from raft.messaging import (
    AppendEntriesSucceeded,
    Message,
    InvalidTerm,
)


def test_handle_message_invalid_term(no_network_raft_follower):
    """AppendEntries receiver implementation bullet 1"""
    no_network_raft_follower.term = 1
    result = no_network_raft_follower.handle_message(
        Message(server_no=1, term=0, content=AppendEntriesSucceeded())
    )
    assert isinstance(result, InvalidTerm)


def test_handle_message_valid_term(no_network_raft_follower):
    """AppendEntries receiver implementation bullet 1"""
    with mock.patch.object(no_network_raft_follower, 'handle_append_entries_succeeded') as mocked_method:
        no_network_raft_follower.handle_message(
            Message(server_no=1, term=0, content=AppendEntriesSucceeded())
        )
        assert mocked_method.called
