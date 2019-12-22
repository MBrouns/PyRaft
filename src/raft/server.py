import logging
import pickle
import queue
from collections import Iterable
from enum import Enum
from functools import wraps

from raft.log import Log, LogNotCaughtUpError, LogDifferentTermError, LogEntry
from raft.messaging import (
    Message,
    AppendEntries,
    AppendEntriesFailed,
    AppendEntriesSucceeded,
    InvalidTerm,
    RequestVote,
    VoteGranted,
    VoteDenied,
    Command, NotTheLeader, Result, NoOp, GetValue, SetValue, DelValue)


class State(Enum):
    LEADER = 1
    CANDIDATE = 2
    FOLLOWER = 3


def only(*states, silent=False):
    if not isinstance(states, Iterable):
        states = {states}

    def wrapper(func):
        @wraps(func)
        def impl(self, *args, **kwargs):
            if self.state not in states:
                if not silent:
                    self._logger.info(
                        f"attempted to call {func.__name__} but current state is {self.state}, not in {states}"
                    )
                return
            return func(self, *args, **kwargs)

        return impl

    return wrapper


class RaftServer:
    def __init__(self, server_no, num_servers, state_machine, persist=False):
        self.server_no = server_no
        self._logger = logging.getLogger(f"RaftServer-{server_no}")
        self.num_servers = num_servers
        self._state_machine = state_machine

        self.persist = persist
        self.server_state_path = f'server-{server_no}.state'

        # Persistent state
        self._term = 0
        self.voted_for = None
        self.log = Log()

        self._commit_index = -1
        self.last_applied = -1
        self.leader_id = None
        self.received_votes = set()

        self.state = State.FOLLOWER
        self.outbox = queue.Queue()

        # volatile leader state
        self.next_index = None
        self.match_index = None
        self.unhandled_client_requests = {}

        self.recover()
    @property
    def commit_index(self):
        return self._commit_index

    @commit_index.setter
    def commit_index(self, value):
        self._commit_index = value
        while value > self.last_applied:
            self.last_applied += 1
            operation = self.log[self.last_applied].msg
            self._state_machine.apply(operation)
            if operation in self.unhandled_client_requests:
                self._send(self.unhandled_client_requests[operation], Result('ok'))

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
        self.match_index = [-1 for _ in range(self.num_servers)]
        self.match_index[self.server_no] = len(self.log)
        self.leader_log_append(NoOp(request_id=0))

    def become_follower(self):
        self.state = State.FOLLOWER
        self.unhandled_client_requests = {}

    def become_candidate(self):
        self.state = State.CANDIDATE
        self.term += 1

        self._logger.info(f"Transitioned to candidate in term {self.term}")
        self.voted_for = self.server_no
        self.received_votes.add(self.server_no)
        self.outbox.put("reset_election_timeout")
        self.request_votes()

    @only(State.CANDIDATE)
    def request_votes(self):
        followers = [
            server for server in range(self.num_servers) if server != self.server_no
        ]
        for server_no in followers:
            self._send(
                server_no,
                RequestVote(
                    term=self.term,
                    candidate_log_len=len(self.log),
                    last_log_term=self.log.last_term,
                ),
            )

    # @only([State.CANDIDATE, State.FOLLOWER], silent=True)
    def handle_election_timeout(self):
        if self.state == State.LEADER:
            pass
        self.become_candidate()

    @only(State.LEADER, silent=True)
    def handle_heartbeat(self):
        self.outbox.put("reset_election_timeout")

        followers = [
            server for server in range(self.num_servers) if server != self.server_no
        ]
        for server_no in followers:
            append_entries_msg = self._append_entries_msg(server_no)
            if append_entries_msg:
                self._send(server_no, self._append_entries_msg(server_no))

    @only(State.LEADER)
    def leader_log_append(self, entry):
        self.log.append(len(self.log), prev_log_term=self.log.last_term, entry=LogEntry(self.term, entry))
        self.match_index[self.server_no] = len(self.log)

    def _handle_command(self, client_id, operation):
        # command is any of SetValue, GetValue, DelValue, NoOp
        if self.state != State.LEADER:
            self._send(client_id, NotTheLeader(self.leader_id))
            return

        if isinstance(operation, NoOp):
            self._send(client_id, Result('ok'))
            return

        if isinstance(operation, GetValue):
            result = self._state_machine.apply(operation)
            self._send(client_id, Result(result))
            return

        # Idempotency check
        for log_index, log_entry in enumerate(self.log):
            if log_entry.msg.request_id == operation.request_id:
                if log_index <= self.last_applied:
                    self._send(client_id, Result('ok'))
                else:
                    self.unhandled_client_requests[operation] = client_id

                # Operation was already in the servers log, don't append it again
                return

        if isinstance(operation, (SetValue, DelValue)):
            self.leader_log_append(operation)
            self.unhandled_client_requests[operation] = client_id
            return

        raise ValueError(f"Expected command to be in NoOp, GetValue, SetValue, DelValue, got {type(operation)}")

    def handle_message(self, message: Message):
        message_handlers = {
            AppendEntries: self._handle_append_entries,
            AppendEntriesSucceeded: self._handle_append_entries_succeeded,
            AppendEntriesFailed: self._handle_append_entries_failed,
            RequestVote: self._handle_request_vote,
            VoteGranted: self._handle_vote_granted,
            VoteDenied: self._handle_vote_denied,
            InvalidTerm: lambda *args, **kwargs: None,
            Command: self._handle_command,
        }
        self._logger.info(
            f"Received {message.content} message from server {message.sender}"
        )

        if message.term is not None and message.term < self.term:
            self._logger.info(f"Server {message.sender} has a lower term, ignoring")

            self._send(message.sender, InvalidTerm())
            return

        if message.term is not None and message.term > self.term:
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
            self._send(message.sender, response)

    @only(State.CANDIDATE)
    def _handle_vote_denied(self, server_no, reason):
        self._logger.info(f"did not get vote from server {server_no} because {reason}")

    @only(State.CANDIDATE)
    def _handle_vote_granted(self, voter_id):
        self.received_votes.add(voter_id)

        if len(self.received_votes) > self.num_servers // 2:
            self.become_leader()

    def _handle_request_vote(
        self, candidate_id, term, candidate_log_len, last_log_term
    ):
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
        self.voted_for = candidate_id
        return VoteGranted()

    @only(State.LEADER)
    def _append_entries_msg(self, server_no):
        log_index = self.next_index[server_no]

        if log_index >= len(self.log):
            self._logger.debug(
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

    def _handle_append_entries(
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
            self._logger.info(
                f"received append entries call while current state was {self.state}"
            )
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

    @only(State.LEADER)
    def _handle_append_entries_succeeded(self, other_server_no, replicated_index):
        self.match_index[other_server_no] = replicated_index
        self.next_index[other_server_no] = replicated_index + 1
        self._update_committed_entries()

    def _update_committed_entries(self):
        self._logger.info(f"checking whether log entries can be committed")
        self._logger.info(f"match index: {self.match_index}")

        majority_match_n = sorted(self.match_index)[self.num_servers // 2]
        possible_ns = range(self.commit_index, min(majority_match_n, len(self.log)) + 1)

        self._logger.info(f"possible entries that can be committed: {possible_ns}")
        n = None
        for possible_n in possible_ns:
            if self.log[possible_n].term == self.term:
                n = possible_n

        if n is not None:
            self.commit_index = n

    @only(State.LEADER)
    def _handle_append_entries_failed(self, other_server_no, reason):
        new_try_log_index = self.next_index[other_server_no] - 1

        self._logger.info(
            f"Received an AppendEntriesFailed message from {other_server_no}. Reason was: {reason} "
            f"retrying with log index {new_try_log_index}"
        )

        self.next_index[other_server_no] = new_try_log_index
        return self._append_entries_msg(server_no=other_server_no)

    def write(self):
        if not self.persist:
            return

        with open(self.server_state_path, 'wb') as persisted_state_file:
            pickle.dump({
                '_term': self.term,
                'voted_for': self.voted_for,
                'log': self.log,
                'state': self.state
            }, persisted_state_file)

    def recover(self):
        if not self.persist:
            return
        try:
            with open(self.server_state_path, 'rb') as persisted_state_file:
                state = pickle.load(persisted_state_file)
                self._term = state['_term']
                self.voted_for = state['voted_for']
                self.log = state['log']
        except FileNotFoundError:
            pass

    def _send(self, to, content):
        self.write()
        self.outbox.put(
            Message(
                sender=self.server_no, term=self.term, recipient=to, content=content
            )
        )
