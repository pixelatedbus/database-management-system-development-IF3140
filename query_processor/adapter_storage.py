# gw asumsiin disini manggil make data2 yang diperluin ama class(es) storage yak
from storage_manager import *

class AdapterStorage:
    def __init__(self):
        self.sm = StorageManager()

    def storage_select(self, table_name, conditions=None, projections=None):
        print("\n[STORAGE IFACE] CALLING SELECT")
        print(f"[STORAGE IFACE] Table       : {table_name}")
        print(f"[STORAGE IFACE] Projections : {projections}")
        print(f"[STORAGE IFACE] Conditions  : {conditions}")

        cond_objs = []
        if conditions:
            for col, op, val in conditions:
                print(f"[STORAGE IFACE]  -> Building Condition({col} {op} {val})")
                cond_objs.append(Condition(col, op, val))

        dr = DataRetrieval(
            table=table_name,
            column=projections or [],
            conditions=cond_objs
        )

        print(f"[STORAGE IFACE] DataRetrieval Object:")
        print(f"    table      = {dr.table}")
        print(f"    column     = {dr.column}")
        print(f"    conditions = {[ (c.column, c.operation, c.operand) for c in dr.conditions ]}")

        result = self.sm.read_block(dr)

        print(f"[STORAGE IFACE] SELECT RESULT ({len(result)} rows):")
        for row in result:
            print("   ", row)

        return result

    def storage_insert(self, table_name, row_dict):
        print("\n[STORAGE IFACE] CALLING INSERT")
        print(f"[STORAGE IFACE] Table : {table_name}")
        print(f"[STORAGE IFACE] Row   : {row_dict}")

        cols = list(row_dict.keys())
        vals = list(row_dict.values())

        print(f"[STORAGE IFACE] Columns = {cols}")
        print(f"[STORAGE IFACE] Values  = {vals}")

        dw = DataWrite(
            table=table_name,
            column=cols,
            new_value=vals,
            conditions=[]
        )

        print(f"[STORAGE IFACE] DataWrite Object:")
        print(f"    table   = {dw.table}")
        print(f"    columns = {dw.column}")
        print(f"    values  = {dw.new_value}")
        print(f"    mode    = INSERT")

        out = self.sm.write_block(dw)

        print(f"[STORAGE IFACE] INSERT RESULT : {out} rows inserted")

        return out

    def storage_update(self, table_name, set_dict, condition_tuple):
        print("\n[STORAGE IFACE] CALLING UPDATE")
        print(f"[STORAGE IFACE] Table     : {table_name}")
        print(f"[STORAGE IFACE] Set Dict  : {set_dict}")
        print(f"[STORAGE IFACE] Condition : {condition_tuple}")

        col, op, val = condition_tuple
        cond = Condition(col, op, val)

        print(f"[STORAGE IFACE]  -> Built Condition({col} {op} {val})")

        dw = DataWrite(
            table=table_name,
            column=list(set_dict.keys()),
            new_value=list(set_dict.values()),
            conditions=[cond]
        )

        print(f"[STORAGE IFACE] DataWrite Object:")
        print(f"    table      = {dw.table}")
        print(f"    columns    = {dw.column}")
        print(f"    new_value  = {dw.new_value}")
        print(f"    conditions = {[(cond.column, cond.operation, cond.operand) for cond in dw.conditions]}")
        print(f"    mode       = UPDATE")

        affected = self.sm.write_block(dw)
        print(f"[STORAGE IFACE] UPDATE RESULT : {affected} rows updated")

        return affected

    def storage_delete(self, table_name, condition_tuple):
        print("\n[STORAGE IFACE] CALLING DELETE")
        print(f"[STORAGE IFACE] Table     : {table_name}")
        print(f"[STORAGE IFACE] Condition : {condition_tuple}")

        col, op, val = condition_tuple
        cond = Condition(col, op, val)

        print(f"[STORAGE IFACE]  -> Built Condition({col} {op} {val})")

        dd = DataDeletion(
            table=table_name,
            conditions=[cond]
        )

        print(f"[STORAGE IFACE] DataDeletion Object:")
        print(f"    table      = {dd.table}")
        print(f"    conditions = {[(cond.column, cond.operation, cond.operand) for cond in dd.conditions]}")

        deleted = self.sm.delete_block(dd)

        print(f"[STORAGE IFACE] DELETE RESULT : {deleted} rows deleted")

        return deleted

