import logging
import queue

from raft.log import Log, LogNotCaughtUpError, LogDifferentTermError
from raft.messaging import (
    Message,
    AppendEntries,
    AppendEntriesFailed,
    AppendEntriesSucceeded,
    InvalidTerm,
    RequestVote,
    VoteGranted,
    VoteDenied,
)
from raft.state_machine import State


class RaftServer:
    def __init__(self, server_no, num_servers):
        self.server_no = server_no
        self._logger = logging.getLogger(f"RaftServer-{server_no}")
        self.num_servers = num_servers

        self._term = 0
        self.commit_index = 0
        self.voted_for = None
        self.leader_id = None
        self.received_votes = set()

        self.log = Log()  # todo: put a lock around it?

        self.state = State.FOLLOWER
        self.outbox = queue.Queue()

        # volatile leader state
        self.next_index = None
        self.match_index = None

    @property
    def term(self):
        return self._term

    @term.setter
    def term(self, value):
        self.voted_for = None
        self._term = value

    def become_leader(self):
        self._logger.info(
            f"Transitioned to leader of term {self.term}, setting next_index to {max(len(self.log), 0)}"
        )
        self.state = State.LEADER

        self.next_index = [max(0, len(self.log)) for _ in range(self.num_servers)]
        self.match_index = [0 for _ in range(self.num_servers)]

    def become_follower(self):
        self.state = State.FOLLOWER

    def become_candidate(self):

        self.state = State.CANDIDATE
        self.term += 1

        self._logger.info( f"Transitioned to candidate in term {self.term}")
        self.voted_for = self.server_no
        self.received_votes.add(self.server_no)
        self.outbox.put("reset_election_timeout")
        self.request_votes()

    def request_votes(self):
        if self.state != State.CANDIDATE:
            return
        followers = [
            server for server in range(self.num_servers) if server != self.server_no
        ]
        for server_no in followers:
            self.send(
                server_no,
                RequestVote(
                    term=self.term,
                    candidate_log_len=len(self.log),
                    last_log_term=self.log.last_term,
                ),
            )

    def handle_election_timeout(self):
        if self.state == State.LEADER:
            return

        self.become_candidate()

    def handle_vote_granted(self, voter_id):
        if self.state != State.CANDIDATE:
            return

        self.received_votes.add(voter_id)

        if len(self.received_votes) > self.num_servers // 2:
            self.become_leader()

    def send(self, to, content):
        self.outbox.put(
            Message(
                sender=self.server_no, term=self.term, recipient=to, content=content
            )
        )

    def handle_message(self, message: Message):
        message_handlers = {
            AppendEntries: self.handle_append_entries,
            AppendEntriesSucceeded: self.handle_append_entries_succeeded,
            AppendEntriesFailed: self.handle_append_entries_failed,
            RequestVote: self.handle_request_vote,
            VoteGranted: self.handle_vote_granted,
            VoteDenied: self.handle_vote_denied,
            InvalidTerm: lambda *args, **kwargs: None,
        }
        self._logger.info(
            f"Received {message.content} message from server {message.sender}"
        )

        if message.term < self.term:
            self._logger.info(f"Server {message.sender} has a lower term, ignoring")
            self.send(message.sender, InvalidTerm())

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
        if response is not None:
            self.send(message.sender, response)

    def handle_vote_denied(self, server_no, reason):
        if self.state != State.CANDIDATE:
            return
        self._logger.info(f"did not get vote from server {server_no} because {reason}")

    def handle_request_vote(self, candidate_id, term, candidate_log_len, last_log_term):
        if term < self.term:
            return VoteDenied(
                f"Vote came from server on term {term} while own term was {self.term}"
            )

        if not (self.voted_for is None or self.voted_for == candidate_id):
            return VoteDenied(f"already voted for other candidate: {self.voted_for}")

        if candidate_log_len < len(self.log):
            return VoteDenied(
                f"candidate log was size {candidate_log_len} while own log length was {len(self.log)}"
            )

        if last_log_term < self.log.last_term:
            return VoteDenied(
                f"candidate log was on term {last_log_term} while own log length was {self.log.last_term}"
            )

        self.outbox.put("reset_election_timeout")
        return VoteGranted()

    def _append_entries_msg(self, server_no):
        if not self.state == State.LEADER:
            return

        log_index = self.next_index[server_no]

        if log_index >= len(self.log):
            self._logger.info(
                f"{server_no} already has all log entries {log_index}/{len(self.log)}"
            )
            return AppendEntries(
                log_index=log_index,
                prev_log_term=self.log.last_term,
                entry=None,
                leader_commit=self.commit_index,
            )

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
        followers = [
            server for server in range(self.num_servers) if server != self.server_no
        ]
        for server_no in followers:
            append_entries_msg = self._append_entries_msg(server_no)
            if append_entries_msg:
                self.send(server_no, self._append_entries_msg(server_no))

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
        if self.state != State.FOLLOWER:
            self._logger.info(f"received append entries call while current state was {self.state}")
            self.become_follower()

        self.outbox.put("reset_election_timeout")
        self.leader_id = leader_id
        try:
            replicated_index = self.log.append(
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
            self.commit_index = min(leader_commit, replicated_index)

        return AppendEntriesSucceeded(replicated_index)

    def handle_append_entries_succeeded(self, other_server_no, replicated_index):
        if self.state != State.LEADER:
            self._logger.info(
                f"Received an AppendEntriesSucceeded message from {other_server_no}"
                f"but current state is not leader. Ignoring the message"
            )
            return

        self.match_index[other_server_no] = replicated_index
        self.next_index[other_server_no] = replicated_index + 1

    def handle_append_entries_failed(self, other_server_no, reason):
        if self.state != State.LEADER:
            self._logger.info(
                f"Received an AppendEntriesFailed message from {other_server_no} "
                f"but current state is not leader. Ignoring the message"
            )
            return

        new_try_log_index = self.next_index[other_server_no] - 1

        self._logger.info(
            f"Received an AppendEntriesFailed message from {other_server_no}. Reason was: {reason} "
            f"retrying with log index {new_try_log_index}"
        )

        self.next_index[other_server_no] = new_try_log_index
        return self._append_entries_msg(server_no=other_server_no)
