import logging
import queue
import random
import threading
import time

from raft import config
from raft.log import LogEntry
from raft.messaging import Message


class RaftController:
    def __init__(self, server_no, machine, net, state_machine):
        self._logger = logging.getLogger(f"RaftController-{server_no}")
        self._events = queue.Queue()
        self._machine = machine
        self._net = net
        self._state_machine = state_machine

        self.hb_since_election_timeout_lock = threading.Lock()
        self.server_no = server_no

    def start(self):
        self._logger.info("starting network thread")
        self._net.start()
        self._logger.info("starting event loop")
        threading.Thread(
            target=self._event_loop,
            daemon=True,
            name=f"RaftController-{self.server_no}-event-loop",
        ).start()
        self._logger.info(f"starting message receiver")
        threading.Thread(
            target=self._rcv_message_loop,
            daemon=True,
            name=f"RaftController-{self.server_no}-rcv-message-loop",
        ).start()
        self._logger.info(f"starting message sender")
        threading.Thread(
            target=self._handle_machine_events,
            daemon=True,
            name=f"RaftController-{self.server_no}-send-message-loop",
        ).start()
        self._logger.info("starting heartbeat")
        threading.Thread(
            target=self._heartbeat_timer,
            daemon=True,
            name=f"RaftController-{self.server_no}-heartbeat",
        ).start()

    def _rcv_message_loop(self):
        while True:
            msg = self._net.recv()
            self._events.put(("message", msg))

    def _handle_machine_events(self):
        while True:
            raft_server_event = self._machine.outbox.get()

            if isinstance(raft_server_event, LogEntry):
                self._state_machine.apply(raft_server_event)
            elif isinstance(raft_server_event, Message):
                self._net.send(raft_server_event)
            elif raft_server_event == "reset_election_timeout":
                with self.hb_since_election_timeout_lock:
                    self.hb_since_election_timeout = 0
            else:
                raise ValueError(
                    f"controller got unknown event from RaftServer: {raft_server_event}"
                )

    def _heartbeat_timer(self):
        self.hb_since_election_timeout = 0
        hb_for_election_timeout = random.randint(
            config.MIN_TIMEOUT_HEARTBEATS, config.MAX_TIMEOUT_HEARTBEATS
        )
        while True:
            time.sleep(config.LEADER_HEARTBEAT)
            self._logger.info(
                f"HEARTBEAT - {self.hb_since_election_timeout}/{hb_for_election_timeout} for election timeout"
            )
            self._events.put(("heartbeat",))
            with self.hb_since_election_timeout_lock:
                self.hb_since_election_timeout += 1

            if self.hb_since_election_timeout == hb_for_election_timeout:
                self._logger.info(f"Election timeout reached")
                self._events.put(("election_timeout",))
                hb_for_election_timeout = random.randint(
                    config.MIN_TIMEOUT_HEARTBEATS, config.MAX_TIMEOUT_HEARTBEATS
                )

    def _event_loop(self):
        while True:
            evt, *args = self._events.get()
            self._logger.debug("Event %r %r", evt, args)

            if evt == "message":
                self._machine.handle_message(*args)
            elif evt == "election_timeout":
                self._machine.handle_election_timeout()
            elif evt == "heartbeat":
                self._machine.handle_heartbeat()
            else:
                raise RuntimeError("Unknown event")
