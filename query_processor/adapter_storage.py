from storage_manager.storage_manager import StorageManager
from storage_manager.models import Condition, DataRetrieval, DataWrite, DataDeletion

class AdapterStorage:
    def __init__(self):
        self.sm = StorageManager()

    def storage_select(self, table_name, conds=None, projections=None):
        dr = DataRetrieval(
            table=table_name,
            column=projections or [],
            conditions=conds
        )

        result = self.sm.read_block(dr)
        return result

    def storage_insert(self, table_name, row_dict):
        cols = list(row_dict.keys())
        vals = list(row_dict.values())

        dw = DataWrite(
            table=table_name,
            column=cols,
            new_value=vals,
            conditions=[]
        )

        out = self.sm.write_block(dw)

        return out

    def storage_update(self, table_name, set_dict, condition_tuple):

        col, op, val = condition_tuple
        cond = Condition(col, op, val)

        dw = DataWrite(
            table=table_name,
            column=list(set_dict.keys()),
            new_value=list(set_dict.values()),
            conditions=[cond]
        )

        affected = self.sm.write_block(dw)

        return affected

    def storage_delete(self, table_name, condition_tuple):
        col, op, val = condition_tuple
        cond = Condition(col, op, val)

        dd = DataDeletion(
            table=table_name,
            conditions=[cond]
        )

        deleted = self.sm.delete_block(dd)

        return deleted

