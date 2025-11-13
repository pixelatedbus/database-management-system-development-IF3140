class log:
        def __init__(self, transaction_id, action, timestamp, old_data = None, new_data = None, table_name : str = None) -> None:
                self.transaction_id = transaction_id
                self.action = action
                self.timestamp = timestamp
                self.old_data = old_data
                self.new_data = new_data
                self.table_name = table_name


class actiontype(enumerate):
        start = 0
        write = 1
        commit = 2
        abort = 3