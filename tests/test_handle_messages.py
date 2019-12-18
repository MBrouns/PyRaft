from unittest import mock

from raft.messaging import (
    AppendEntriesSucceeded,
    Message,
    InvalidTerm,
)
from raft.state_machine import State


def test_handle_message_invalid_term(no_network_raft_follower):
    """AppendEntries receiver implementation bullet 1"""
    no_network_raft_follower.term = 1
    result = no_network_raft_follower.handle_message(
        Message(sender=1, term=0, content=AppendEntriesSucceeded(1))
    )
    assert isinstance(result, InvalidTerm)


def test_handle_message_same_term(no_network_raft_follower):
    """AppendEntries receiver implementation bullet 1"""
    with mock.patch.object(no_network_raft_follower, 'handle_append_entries_succeeded') as mocked_method:
        no_network_raft_follower.handle_message(
            Message(sender=1, term=0, content=AppendEntriesSucceeded(1))
        )
        assert mocked_method.called


def test_handle_message_higher_term(no_network_raft_leader):
    """AppendEntries receiver implementation bullet 1"""

    result = no_network_raft_leader.handle_message(
        Message(sender=1, term=1, content=AppendEntriesSucceeded(1))
    )
    assert result == (1, None)
    assert no_network_raft_leader.state == State.FOLLOWER
    assert no_network_raft_leader.term == 1
