import logging

from raft.log import Log, LogNotCaughtUpError, LogDifferentTermError
from raft.messaging import (
    Message,
    AppendEntries,
    AppendEntriesFailed,
    AppendEntriesSucceeded,
    InvalidTerm,
)
from raft.state_machine import State


class RaftServer:
    def __init__(self, server_no, num_servers):
        self.server_no = server_no
        self._logger = logging.getLogger(f"RaftServer-{server_no}")
        self.num_servers = num_servers

        self.term = 0
        self.commit_index = 0
        self.leader_id = None

        self.log = Log()  # todo: put a lock around it

        self.state = State.FOLLOWER

        # volatile leader state
        self.next_index = None
        self.match_index = None

    def become_leader(self):
        self._logger.info(
            f"Transitioned to leader, setting next_index to {max(len(self.log) - 1, 0)}"
        )
        self.state = State.LEADER

        self.next_index = [max(0, len(self.log) - 1) for _ in range(self.num_servers)]
        self.match_index = [0 for _ in range(self.num_servers)]

    def handle_message(self, message: Message):
        message_handlers = {
            AppendEntries: self.handle_append_entries,
            AppendEntriesSucceeded: self.handle_append_entries_succeeded,
            AppendEntriesFailed: self.handle_append_entries_failed,
        }
        self._logger.info(
            f"Received {type(message.content)} message from server {message.sender}"
        )

        if message.term < self.term:
            self._logger.info(f"Server {message.sender} has a lower term, ignoring")
            return InvalidTerm()

        if message.term > self.term:
            self._logger.info(
                f"Server {message.sender} has an higher term, updating mine and "
                f"converting to follower"
            )
            self.become_follower()
            self.term = message.term

        try:
            handler = message_handlers[type(message.content)]
        except AttributeError:
            raise ValueError(
                f"unknown message. expected {message_handlers.keys()}, got {type(message)}"
            )

        response = handler(message.sender, **message.content._asdict())
        return message.sender, response

    def _send_append_entries(self, server_no):
        if not self.state == State.LEADER:
            return None

        log_index = self.next_index[server_no]

        if log_index == len(self.log):
            self._logger.info(
                f"{server_no} already has all log entries {log_index}/{log_index}"
            )
            return None

        self._logger.info(
            f"sending AppendEntries RPC to {server_no} for index {log_index}/{len(self.log)}"
        )

        return AppendEntries(
            log_index=log_index,
            prev_log_term=self.log[log_index - 1].term,
            entry=self.log[log_index],
            leader_commit=self.commit_index,
        )

    def handle_heartbeat(self):
        for server_no in range(self.num_servers):
            if server_no != self.server_no:
                resp = self._send_append_entries(server_no)
                if resp is not None:
                    yield server_no, resp

    def handle_append_entries(
        self, leader_id, log_index, prev_log_term, entry, leader_commit
    ):
        """
        Args:
            leader_id: so follower can redirect clients
            log_index: index in the log where to append to
            prev_log_term: according to the leader, the term of the last log entry
            entry: the entry to add
            leader_commit: the leaders' commit_index

        Returns:

        """
        if not self.state != State.FOLLOWER:
            self._logger.info(f"received append entries call while current state ")
            self.become_follower()

        self.leader_id = leader_id
        try:
            self.log.append(
                log_index=log_index, prev_log_term=prev_log_term, entry=entry
            )
        except (LogNotCaughtUpError, LogDifferentTermError) as e:
            return AppendEntriesFailed(reason=e)

        # The min in the final step (5) of AppendEntries is necessary, and it needs to be computed with the index of the
        # last new entry. It is not sufficient to simply have the function that applies things from your log between
        # lastApplied and commitIndex stop when it reaches the end of your log. This is because you may have entries in
        # your log that differ from the leader’s log after the entries that the leader sent you (which all match the
        # ones in your log). Because #3 dictates that you only truncate your log if you have conflicting entries,
        # those won’t be removed, and if leaderCommit is beyond the entries the leader sent you,
        # you may apply incorrect entries.
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, log_index)

        return AppendEntriesSucceeded(log_index)

    def handle_append_entries_succeeded(self, other_server_no, replicated_index):
        if self.state != State.LEADER:
            self._logger.info(
                f"Received an AppendEntriesSucceeded message from {other_server_no}"
                f"but current state is not leader. Ignoring the message"
            )
            return None

        self.match_index[other_server_no] = replicated_index
        self.next_index[other_server_no] = replicated_index + 1

    def handle_append_entries_failed(self, other_server_no, reason):
        if self.state != State.LEADER:
            self._logger.info(
                f"Received an AppendEntriesFailed message from {other_server_no}"
                f"but current state is not leader. Ignoring the message"
            )
            return None

        new_try_log_index = self.next_index[other_server_no] - 1

        self._logger.info(
            f"Received an AppendEntriesFailed message from {other_server_no}. Reason was: {reason}"
            f"retrying with log index {new_try_log_index}"
        )

        self.next_index[other_server_no] = new_try_log_index
        return self._send_append_entries(server_no=other_server_no)
