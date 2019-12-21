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

        self._pause_signal = threading.Event()
        self._pause_signal.set()
        self.hb_since_election_timeout_lock = threading.Lock()
        self.server_no = server_no
        self._threads = {}

    def pause(self):
        self._pause_signal.clear()

    def unpause(self):
        self._pause_signal.set()

    def start(self):
        self._logger.info("starting network thread")
        self._net.start()
        self._logger.info("starting event loop")
        self._threads['event-loop'] = threading.Thread(
            target=self._event_loop,
            daemon=True,
            name=f"RaftController-{self.server_no}-event-loop",
        )
        self._logger.info(f"starting message receiver")
        self._threads['rcv-message'] = threading.Thread(
            target=self._rcv_message_loop,
            daemon=True,
            name=f"RaftController-{self.server_no}-rcv-message-loop",
        )
        self._logger.info(f"starting message sender")
        self._threads['send-message'] = threading.Thread(
            target=self._handle_machine_events,
            daemon=True,
            name=f"RaftController-{self.server_no}-send-message-loop",
        )
        self._logger.info("starting heartbeat")
        self._threads['heartbeat'] = threading.Thread(
            target=self._heartbeat_timer,
            daemon=True,
            name=f"RaftController-{self.server_no}-heartbeat",
        )
        for thread in self._threads.values():
            thread.start()

    def _rcv_message_loop(self):
        while True:
            msg = self._net.recv()
            self._events.put(("message", msg))

    def _handle_machine_events(self):
        while True:
            raft_server_event = self._machine.outbox.get()

            if isinstance(raft_server_event, Message):
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
        self.hb_for_election_timeout = random.randint(
            config.MIN_TIMEOUT_HEARTBEATS, config.MAX_TIMEOUT_HEARTBEATS
        )
        while True:
            time.sleep(config.LEADER_HEARTBEAT)
            self._logger.info(
                f"HEARTBEAT - {self.hb_since_election_timeout}/{self.hb_for_election_timeout} for election timeout"
            )
            self._events.put(("heartbeat",))
            with self.hb_since_election_timeout_lock:
                self.hb_since_election_timeout += 1

            if self.hb_since_election_timeout >= self.hb_for_election_timeout:
                self._logger.info(f"Election timeout reached")
                self._events.put(("election_timeout",))
                self.hb_for_election_timeout = random.randint(
                    config.MIN_TIMEOUT_HEARTBEATS, config.MAX_TIMEOUT_HEARTBEATS
                )

    def healthy(self):
        for thread_name, thread in self._threads.items():
            thread.join(timeout=0.0)
            if not thread.is_alive():
                return False
        return True

    def _event_loop(self):
        while True:
            self._pause_signal.wait()
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
