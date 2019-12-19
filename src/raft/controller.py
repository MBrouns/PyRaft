import logging
import queue
import random
import threading
import time

from raft import config
from raft.messaging import Message


class RaftController:
    def __init__(self, server_no, machine, net):
        self._logger = logging.getLogger(f"RaftController-{server_no}")
        self._events = queue.Queue()
        self._machine = machine
        self._net = net

        self.heartbeats_since_election_timeout_lock = threading.Lock()
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
            state_machine_event = self._machine.outbox.get()
            if isinstance(state_machine_event, Message):
                self._net.send(state_machine_event)
            elif state_machine_event == "reset_election_timeout":
                with self.heartbeats_since_election_timeout_lock:
                    self.heartbeats_since_election_timeout = 0

    def _heartbeat_timer(self):
        self.heartbeats_since_election_timeout = 0
        heartbeats_for_election_timeout = random.randint(config.MIN_TIMEOUT_HEARTBEATS, config.MAX_TIMEOUT_HEARTBEATS)
        while True:
            time.sleep(config.LEADER_HEARTBEAT)
            self._logger.info(f"HEARTBEAT - {self.heartbeats_since_election_timeout}/{heartbeats_for_election_timeout} for election timeout")
            self._events.put(("heartbeat",))
            with self.heartbeats_since_election_timeout_lock:
                self.heartbeats_since_election_timeout += 1

            if self.heartbeats_since_election_timeout >= heartbeats_for_election_timeout:
                self._logger.info(f"Election timeout reached")
                self._events.put(("election_timeout",))
                heartbeats_for_election_timeout = random.randint(config.MIN_TIMEOUT_HEARTBEATS, config.MAX_TIMEOUT_HEARTBEATS)

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


