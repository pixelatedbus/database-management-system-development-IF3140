import datetime

class RecoveryCriteria:
    def __init__(self,transaction_id: int = None, timestamp: datetime = None):

        if (transaction_id != None and timestamp != None):
            raise("both timestamp and id is set to None!")

        self.transaction_id = transaction_id
        self.timestamp = timestamp