class FailureRecovery:
    def __init__(self):
        self.buffer = []

    def write_log(self, executionResult):
        pass

    def recover(self, criteria):
        pass

    def _save_checkpoint(self):
        pass

class RecoverCriteria:
    def __init__(self, timestamp, transaction_id):
        self.timestamp = timestamp
        self.transaction_id = transaction_id
