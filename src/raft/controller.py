import logging
import queue
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
            target=self._send_message_loop,
            daemon=True,
            name=f"RaftController-{self.server_no}-send-message-loop",
        ).start()
        self._logger.info("starting heartbeat")
        threading.Thread(
            target=self._heartbeat_timer,
            daemon=True,
            name=f"RaftController-{self.server_no}-heartbeat",
        ).start()
        self._logger.info("starting election timeout")
        threading.Thread(
            target=self._election_timer,
            daemon=True,
            name=f"RaftController-{self.server_no}-election-timeout",
        ).start()

    def _rcv_message_loop(self):
        while True:
            msg = self._net.recv()
            self._events.put(("message", msg))

    def _send_message_loop(self):
        while True:
            message = self._machine.outbox.get()
            self._net.send(message)

    def _heartbeat_timer(self):
        while True:
            time.sleep(config.LEADER_HEARTBEAT)
            self._events.put(("heartbeat",))

    def _election_timer(self):
        pass

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


