import queue
from unittest import mock

from raft.log import LogEntry, Log
from raft.messaging import Message


def replicate(leader, follower):
    """Mocks the messages transferring between leaders and followers to do a full cycle of log replication"""

    leader_response_msg = Message(
        sender=leader.server_no,
        term=leader.term,
        recipient=follower.server_no,
        content=leader._append_entries_msg(follower.server_no),
    )

    while leader_response_msg is not None:
        follower.handle_message(leader_response_msg)

        follower_reply = follower.outbox.get()
        while not isinstance(follower_reply, Message):
            follower_reply = follower.outbox.get()
        leader.handle_message(follower_reply)
        try:
            leader_response_msg = leader.outbox.get(False)
            while not isinstance(leader_response_msg, Message):
                leader_response_msg = leader.outbox.get(False)
        except queue.Empty:
            msg = leader._append_entries_msg(follower.server_no)
            if msg.entry is None:  # already replicated
                break
            leader._send(follower, msg)
            leader_response_msg = leader.outbox.get()
            while not isinstance(leader_response_msg, Message):
                leader_response_msg = leader.outbox.get()

    follower._handle_append_entries(leader.server_no, **leader._append_entries_msg(follower.server_no)._asdict())

def logs_same(log1, log2):
    if len(log1) != len(log2):
        return False, f"logs different length: {len(log1)} - {len(log2)}"
    for i in range(len(log2)):
        if log1[i] != log2[i]:
            return False, f"logs different in ind ex {i}"
    return True


def test_figure_7(raft_cluster):
    leader, a, b, c, d, e, f = raft_cluster(7)
    leader_log_entries = [
        LogEntry(1, "1"),
        LogEntry(1, "2"),
        LogEntry(1, "3"),
        LogEntry(4, "4"),
        LogEntry(4, "5"),
        LogEntry(5, "6"),
        LogEntry(5, "7"),
        LogEntry(6, "8"),
        LogEntry(6, "9"),
        LogEntry(6, "10"),
    ]
    leader.log = Log.from_entries(leader_log_entries)
    a.log = Log.from_entries(leader_log_entries[:-1])
    b.log = Log.from_entries(leader_log_entries[:4])
    c.log = Log.from_entries(leader_log_entries + [LogEntry(6, "11")])
    d.log = Log.from_entries(
        leader_log_entries + [LogEntry(7, "11"), LogEntry(7, "11")]
    )
    e.log = Log.from_entries(
        leader_log_entries[:5] + [LogEntry(4, "6"), LogEntry(4, "7")]
    )
    f_log_entries = [
        LogEntry(2, "4"),
        LogEntry(2, "5"),
        LogEntry(2, "6"),
        LogEntry(3, "7"),
        LogEntry(3, "8"),
        LogEntry(3, "9"),
        LogEntry(3, "10"),
        LogEntry(3, "11"),
    ]
    f.log = Log.from_entries(leader_log_entries[:3] + f_log_entries)

    with mock.patch.object(leader, 'leader_log_append') as mocked:
        leader.become_leader()
    replicate(leader, a)
    replicate(leader, b)
    replicate(leader, c)
    replicate(leader, d)
    replicate(leader, e)
    replicate(leader, f)

    assert logs_same(leader.log, a.log)
    assert logs_same(leader.log, b.log)
    assert not logs_same(leader.log, c.log)[0]
    assert len(c.log) - len(leader.log) == 1
    assert not logs_same(leader.log, d.log)[0]
    assert len(d.log) - len(leader.log) == 2
    assert logs_same(leader.log, e.log)
    assert logs_same(leader.log, f.log)


def test_figure_8(raft_cluster):
    r0, r1, r2, r3, r4 = raft_cluster(5)

    # State a:
    r0.term = 1
    r1.term = 1
    r2.term = 1
    r3.term = 1
    r4.term = 1
    with mock.patch.object(r0, 'leader_log_append') as mocked:
        r0.become_leader()
    r0.log.append(log_index=0, prev_log_term=0, entry=LogEntry(r0.term, "1"))
    replicate(r0, r1)
    replicate(r0, r2)
    replicate(r0, r3)
    replicate(r0, r4)

    r0.term = 2
    r1.term = 2
    r0.log.append(log_index=1, prev_log_term=1, entry=LogEntry(r0.term, "2"))
    replicate(r0, r1)
    assert logs_same(r0.log, r1.log)
    assert r0.commit_index == 0

    # State b:
    r0.become_follower()
    r4.term = 3
    with mock.patch.object(r4, 'leader_log_append') as mocked:
        r4.become_leader()
    r4.log.append(log_index=1, prev_log_term=1, entry=LogEntry(r4.term, "3"))
    assert r4.commit_index == 0

    # state c:
    r4.become_follower()
    with mock.patch.object(r0, 'leader_log_append') as mocked:
        r0.become_leader()
    r0.term = 4
    replicate(r0, r2)
    assert logs_same(r0.log, r2.log)
    r0.log.append(log_index=2, prev_log_term=2, entry=LogEntry(r0.term, "4"))
    assert r0.commit_index == 0

    # state d:
    r0.become_follower()
    with mock.patch.object(r4, 'leader_log_append') as mocked:
        r4.become_leader()
    r4.term = 5

    replicate(r4, r0)
    replicate(r4, r1)
    replicate(r4, r2)
    replicate(r4, r3)
    assert logs_same(r4.log, r1.log)
    assert logs_same(r4.log, r2.log)
    assert logs_same(r4.log, r3.log)
    assert len(r4.log) == 2
    assert r4.commit_index == 0

    # new state;
    r4.log.append(log_index=2, prev_log_term=3, entry=LogEntry(r0.term, "4"))
    replicate(r4, r0)
    replicate(r4, r1)
    replicate(r4, r2)
    replicate(r4, r3)

    assert logs_same(r4.log, r1.log)
    assert logs_same(r4.log, r2.log)
    assert logs_same(r4.log, r3.log)
    assert len(r4.log) == 3
    assert r4.commit_index == 2
    assert r3.commit_index == 2
    assert r2.commit_index == 2
    assert r1.commit_index == 0
    assert r0.commit_index == 0


def test_replicate_to_empty_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    """Tests whether log replication works when follower log is empty"""
    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert logs_same(no_network_raft_leader_with_log.log, no_network_raft_follower.log)


def test_replicate_to_complete_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    no_network_raft_follower.log._log = list(no_network_raft_leader_with_log.log._log)
    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert logs_same(no_network_raft_leader_with_log.log, no_network_raft_follower.log)


def test_replicate_to_partial_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    """Test whether log replication succesfully completes when starting with a partial log"""
    no_network_raft_follower.log._log = [
        no_network_raft_leader_with_log.log[0],
        no_network_raft_leader_with_log.log[1],
    ]

    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert logs_same(no_network_raft_leader_with_log.log, no_network_raft_follower.log)


def test_replicate_to_diverging_log(
    no_network_raft_leader_with_log, no_network_raft_follower
):
    """Tests whether log replication recovers after a split brain"""
    no_network_raft_follower.log.append(
        log_index=0, prev_log_term=0, entry=LogEntry(term=0, msg=0)
    )
    no_network_raft_follower.log.append(
        log_index=1, prev_log_term=0, entry=LogEntry(term=0, msg=1)
    )
    no_network_raft_follower.log.append(
        log_index=2, prev_log_term=0, entry=LogEntry(term=0, msg=2)
    )
    no_network_raft_follower.log.append(
        log_index=3, prev_log_term=0, entry=LogEntry(term=0, msg=3)
    )
    no_network_raft_follower.log.append(
        log_index=4, prev_log_term=0, entry=LogEntry(term=0, msg=4)
    )
    no_network_raft_follower.log.append(
        log_index=5, prev_log_term=0, entry=LogEntry(term=0, msg=5)
    )

    replicate(no_network_raft_leader_with_log, no_network_raft_follower)

    assert logs_same(no_network_raft_leader_with_log.log, no_network_raft_follower.log)
