from buffer import buffer, table
from log import actiontype, log
from recovery_criteria import RecoveryCriteria

class FailureRecovery:
    def __init__(self):
        self.buffer = []

    def write_log(self, info):
        # Info type ExecutionResult
        pass

    def recover(self, criteria: RecoveryCriteria = None):
        pass

    def _save_checkpoint(self):
        pass
