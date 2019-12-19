import logging


class LoggerStateMachine:
    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def apply(self, log_entry):
        self._logger.info(f"applying {log_entry} to state_machine")
