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
        self._logger.info(f'starting network thread')
        self._net.start()
        self._logger.info(f'starting event loop')
        threading.Thread(target=self._event_loop, daemon=True).start()
        self._logger.info(f'starting message receiver')
        threading.Thread(target=self._message_receiver, daemon=True).start()
        self._logger.info(f'starting heartbeat')
        threading.Thread(target=self._heartbeat_timer, daemon=True).start()
        self._logger.info(f'starting election timeout')
        threading.Thread(target=self._election_timer, daemon=True).start()

    def _message_receiver(self):
        while True:
            msg = self._net.recv()
            self._events.put(("message", msg))

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
                resp = self._machine.handle_message(*args)
                if resp[1] is not None:
                    self.send(*resp)
            elif evt == "election_timeout":
                self._machine.handle_election_timeout()
            elif evt == "heartbeat":
                for resp in self._machine.handle_heartbeat():
                    if resp is not None:
                        self.send(*resp)
            else:
                raise RuntimeError("Unknown event")

    def send(self, to, content):
        self._net.send(
            to, Message(sender=self.server_no, term=self._machine.term, content=content)
        )
