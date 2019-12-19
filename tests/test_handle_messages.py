from unittest import mock

from raft.messaging import (
    AppendEntriesSucceeded,
    Message,
    InvalidTerm,
)
from raft.server import State


def test_handle_message_invalid_term(no_network_raft_follower):
    """AppendEntries receiver implementation bullet 1"""
    no_network_raft_follower.term = 1
    no_network_raft_follower.handle_message(
        Message(sender=1, term=0, recipient=no_network_raft_follower.server_no, content=AppendEntriesSucceeded(1))
    )
    assert isinstance(no_network_raft_follower.outbox.get().content, InvalidTerm)


def test_handle_message_same_term(no_network_raft_follower):
    """AppendEntries receiver implementation bullet 1"""
    with mock.patch.object(no_network_raft_follower, '_handle_append_entries_succeeded') as mocked_method:
        with mock.patch.object(no_network_raft_follower, '_send') as _:
            no_network_raft_follower.handle_message(
                Message(sender=1, term=0, recipient=no_network_raft_follower.server_no, content=AppendEntriesSucceeded(1))
            )
            assert mocked_method.called


def test_handle_message_higher_term(no_network_raft_leader):
    """AppendEntries receiver implementation bullet 1"""

    no_network_raft_leader.handle_message(
        Message(sender=1, term=1, recipient=no_network_raft_leader.server_no, content=AppendEntriesSucceeded(1))
    )
    assert no_network_raft_leader.outbox.empty()
    assert no_network_raft_leader.state == State.FOLLOWER
    assert no_network_raft_leader.term == 1
