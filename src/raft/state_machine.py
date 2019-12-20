import logging
from raft.messaging import SetValue, DelValue, NoOp, GetValue


class LoggerStateMachine:
    def __init__(self, server_no):
        self._logger = logging.getLogger(f"LoggerStateMachine-{server_no}")

    def apply(self, log_entry):
        self._logger.info(f"applying {log_entry} to state_machine")


class KVStateMachine:
    def __init__(self, server_no):
        self._state = {}
        self._logger = logging.getLogger(f"LoggerStateMachine-{server_no}")

    def apply(self, operation):
        if isinstance(operation, SetValue):
            self._state[operation.key] = operation.value
        elif isinstance(operation, DelValue):
            del self._state[operation.key]
        elif isinstance(operation, NoOp):
            pass
        elif isinstance(operation, GetValue):
            return self._state.get(operation.key)
        else:
            raise ValueError(f"expected operation to be in {SetValue, DelValue, GetValue, NoOp}, got {type(operation)}")


