from typing import NamedTuple


class LogNotCaughtUpError(Exception):
    pass


class LogDifferentTermError(Exception):
    pass


LogEntry = NamedTuple("LogEntry", term=int, msg=str)


class Log:
    def __init__(self):
        self._log = []

    def append(self, log_index, prev_log_term, entry):
        if not isinstance(entry, LogEntry):
            raise ValueError(f"expected a LogEntry instance, got {type(entry)} instead")
        if log_index > len(self):
            raise LogNotCaughtUpError(
                f"tried to assign to index {log_index} but log was only length {len(self)}"
            )
        if log_index != 0 and self[log_index - 1].term != prev_log_term:
            raise LogDifferentTermError(
                f"Tried to assign to log where previous entries term was {self[log_index - 1].term}"
                f"but prev_log_term was {prev_log_term}"
            )

        # If an existing entry conflicts with a new one (same index but different terms),
        # delete the existing entry and all that follow it.

        # The if here is crucial. If the follower has all the entries the leader sent, the follower MUST NOT
        # truncate its log. Any elements following the entries sent by the leader MUST be kept. This is because we
        # could be receiving an outdated AppendEntries RPC from the leader, and truncating the log would mean
        # “taking back” entries that we may have already told the leader that we have in our log.
        if len(self) > log_index and self[log_index].term != entry.term:
            self._log[log_index:] = [entry]
        else:
            self._log.append(entry)

    def __getitem__(self, item):
        return self._log[item]

    def __len__(self):
        return len(self._log)