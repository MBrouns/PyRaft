import logging
from typing import NamedTuple

from raft.log import LogEntry


class LoggerStateMachine:
    def __init__(self, server_no):
        self._logger = logging.getLogger(f"LoggerStateMachine-{server_no}")

    def apply(self, log_entry):
        self._logger.info(f"applying {log_entry} to state_machine")


SetValue = NamedTuple("SetValue", key=str, value=str)
GetValue = NamedTuple("GetValue", key=str)
DelValue = NamedTuple("DelValue", key=str)
NoOp = NamedTuple("NoOp")


class KVStateMachine:
    def __init__(self, server_no):
        self._state = {}
        self._logger = logging.getLogger(f"LoggerStateMachine-{server_no}")

    def apply(self, log_entry):
        operation = log_entry.msg
        if isinstance(operation, SetValue):
            self._state[operation.key] = operation.value
        elif isinstance(operation, DelValue):
            del self._state[operation.key]
        elif isinstance(operation, NoOp):
            pass


