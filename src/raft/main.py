from typing import List


class Server:
    """Base class for all states a server can have in raft protocol"""
    def __init__(self, servers: List["Server"]):
        self.servers = servers

        # These should be persisted on disk before responding to RPCs
        self.current_term = 0  # latest term server has seen
        # candidateId that received vote in current term (or null if none)
        self.voted_for = None

        # log entries; each entry contains command for state machine, and term when entry was received
        # by leader. one indexed in raft paper, does that matter?
        self.log = []

        # Volatile state
        # index of highest log entry known to be committed
        self.commit_index = 0
        # index of highest log entry applied to state machine
        self.last_applied = 0


class Leader(Server):
    def __init__(self):

        # State should be reinitialized after election
        # for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
        self.next_index = [self.last_applied for _ in range(len(self.servers))]
        # for each server, index of highest log entry known to be replicated on server
        self.match_index = [0 for _ in range(len(self.servers))]


class Follower(Server):
    def append_entries(
            self,
            term: int,
            leader_id: int,
            prev_log_index: int,
            prev_log_term: int,
            entries: List,
            leader_commit: int,
    ):
        """
        Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).

        1. Reply false if term < current_rerm (§5.1)
        2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prev_log_term (§5.3)
        3. If an existing entry conflicts with a new one (same index but different terms),
        delete the existing entry and all that follow it (§5.3)
        4. Append any new entries not already in the log
        5. If leader_commit > commit_index, set commit_index = min(leader_commit, index of last new entry)

        Args:
            term: leader’s term
            leader_id: id of the leader, so follower can redirect clients
            prev_log_index: index of log entry immediately preceding new ones
            prev_log_term: term of prevLogIndex entry
            entries: log entries to store (empty for heartbeat; may send more than one for efficiency)
            leader_commit: leader’s commit_index

        Returns:
            term: current_term, for leader to update itself
            success: true if follower contained entry matching prevLogIndex and prevLogTerm
        """
        pass

    def request_vote(
            self, term: int, candidate_id: int, last_log_index: int, last_log_term: int
    ):
        """
        Invoked by candidates to gather votes

        1. Reply false if term < currentTerm (§5.1)
        2. If voted_for is null or candidate_id, and candidate’s log is at least as up-to-date as receiver’s log,
        grant vote (§5.2, §5.4)
        Args:
            term: candidate’s term
            candidate_id: candidate requesting vote
            last_log_index: index of candidate’s last log entry (§5.4)
            last_log_term: term of candidate’s last log entry (§5.4)

        Returns:
            term: current_term, for candidate to update itself
            vote_granted: boolean indicating whether candidate received vote

        """
        pass


class Candidate(Server):
    pass
