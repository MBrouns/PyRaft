import logging
import time

from raft import config
from raft.log import Log, LogEntry, LogNotCaughtUpError, LogDifferentTermError
from raft.messaging import (
    Message,
    AppendEntries,
    AppendEntriesFailed,
    AppendEntriesSucceeded,
    InvalidTerm,
)
from raft.network import SockBackend
from raft.state_machine import State


class RaftServer:
    def __init__(self, server_no, num_servers, net, state_machine):
        self.server_no = server_no
        self._logger = logging.getLogger(f"RaftServer-{server_no}")
        self.num_servers = num_servers

        self.term = 0
        self.commit_index = 0
        self.leader_id = None

        self.log = Log()  # todo: put a lock around it

        self.state_machine = state_machine

        self._net = net
        self._net.run_server()

        # volatile leader state
        self.next_index = None
        self.match_index = None

    def on_transition_leader(self):
        self.next_index = [len(self.log) for _ in range(self.num_servers)]
        self.match_index = [0 for _ in range(self.num_servers)]

    def recv(self):
        return self._net.recv()

    def send(self, server_no, msg):
        self._net.send(
            server_no, Message(server_no=self.server_no, term=self.term, message=msg)
        )

    def handle_message(self, message: Message):
        message_handlers = {
            AppendEntries: self.handle_append_entries,
            AppendEntriesSucceeded: self.handle_append_entries_succeeded,
            AppendEntriesFailed: self.handle_append_entries_failed,
        }
        # TODO: write test
        if message.term < self.term:
            return InvalidTerm()
        # TODO: write test
        if message.term > self.term:
            # TODO: convert to follower
            self.term = message.term

        try:
            handler = message_handlers[type(message.content)]
        except AttributeError:
            raise ValueError(
                f"unknown message. expected {message_handlers.keys()}, got {type(message)}"
            )

        return handler(message.server_no, **message.content._asdict())

        # TODO: where to do sending?
        # self.send(message.server_no, ret_val)

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
        if not self.state_machine.state != State.FOLLOWER:
            # TODO: demote to follower
            pass

        self.leader_id = leader_id
        try:
            self.log.append(
                log_index=log_index,
                prev_log_term=prev_log_term,
                entry=LogEntry(self.term, entry),
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
        if self.state_machine.state != State.LEADER:
            self._logger.info(f'Received an AppendEntriesSucceeded message from {other_server_no}'
                              f'but current state is not leader. Ignoring the message')
            return False

        self.match_index[other_server_no] = replicated_index
        self.next_index[other_server_no] = replicated_index + 1

    def handle_append_entries_failed(self, other_server_no, reason):
        if self.state_machine.state != State.LEADER:
            self._logger.info(f'Received an AppendEntriesFailed message from {other_server_no}'
                              f'but current state is not leader. Ignoring the message')
            return False

        new_try_log_index = self.next_index[other_server_no] - 1

        self._logger.info(f'Received an AppendEntriesFailed message from {other_server_no}. Reason was: {reason}'
                          f'retrying with log index {new_try_log_index}')

        self.next_index[other_server_no] = new_try_log_index
        return AppendEntries(
            log_index=new_try_log_index,
            prev_log_term=self.log[new_try_log_index - 1].term,
            entry=self.log[new_try_log_index],
            leader_commit=self.commit_index
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(relativeCreated)6d %(threadName)s  - %(name)s - %(levelname)s - %(message)s",
    )
    h0 = RaftServer(0, SockBackend(0, config.SERVERS))
    h1 = RaftServer(1, SockBackend(1, config.SERVERS))

    time.sleep(1)
    h0.send(1, "hello world")
    h0.send(1, "another world")
    print(h1.recv())
