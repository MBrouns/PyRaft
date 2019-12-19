import pytest
from raft.log import Log, LogNotCaughtUpError, LogEntry, LogDifferentTermError


def test_log_wrong_index():
    log = Log()
    with pytest.raises(LogNotCaughtUpError):
        entry = LogEntry(term=0, msg=42)
        log.append(log_index=1, prev_log_term=0, entry=entry)


def test_log_wrong_term():
    log = Log()
    log.append(log_index=0, prev_log_term=0, entry=LogEntry(term=0, msg=42))
    log.append(log_index=1, prev_log_term=0, entry=LogEntry(term=0, msg=42))

    with pytest.raises(LogDifferentTermError):
        entry = LogEntry(term=1, msg=42)
        log.append(log_index=2, prev_log_term=1, entry=entry)


def test_log_append_correct():
    log = Log()
    entry = LogEntry(term=0, msg=42)
    log.append(log_index=0, prev_log_term=0, entry=entry)
    assert log[0] == entry


def test_log_append_none():
    log = Log()
    log.append(log_index=0, prev_log_term=0, entry=None)
    assert len(log) == 0


def test_log_append_invalid_entry():
    log = Log()
    with pytest.raises(ValueError):
        log.append(log_index=0, prev_log_term=0, entry=1)
    assert len(log) == 0


def test_log_append_no_truncate():
    """
    If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all
    that follow it.

    The if here is crucial. If the follower has all the entries the leader sent, the follower MUST NOT truncate its log.
    Any elements following the entries sent by the leader MUST be kept. This is because we could be receiving an
    outdated AppendEntries RPC from the leader, and truncating the log would mean “taking back” entries that we may
    have already told the leader that we have in our log.
    """
    log = Log()
    entry = LogEntry(term=0, msg=42)

    log.append(log_index=0, prev_log_term=0, entry=entry)
    log.append(log_index=1, prev_log_term=0, entry=entry)
    log.append(log_index=2, prev_log_term=0, entry=entry)
    log.append(log_index=0, prev_log_term=0, entry=entry)
    assert log[0] == entry
    assert len(log) == 3


def test_log_append_entries_conflict():
    log = Log()
    entry = LogEntry(term=1, msg=42)

    log.append(log_index=0, prev_log_term=0, entry=LogEntry(term=0, msg=42))
    log.append(log_index=1, prev_log_term=0, entry=LogEntry(term=0, msg=42))
    log.append(log_index=2, prev_log_term=0, entry=LogEntry(term=0, msg=42))
    log.append(log_index=0, prev_log_term=0, entry=entry)
    assert log[0] == entry
    assert len(log) == 1
