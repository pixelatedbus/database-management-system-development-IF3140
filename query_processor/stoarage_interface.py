# Gw asumsi disini inputnya adalah attr dari query tree yang dibutuhin sama Class storage atau
# Kalau nanti ada perubahan di execute_node, infokan saja

from storage_manager import *

sm = StorageManager()

def storage_select(table_name, conditions=None, projections=None):
    """Mengambil data dari storage manager.
    Args:
        table_name (str): Nama tabel yang diakses.
        conditions (list of str): List kondisi dalam bentuk (kolom, operator, nilai).
        projections (list of str): List nama kolom yang ingin diambil.
        
    Returns:
        list of dict: Data yang diambil dalam bentuk list of dictionary.
    """
    cond_objs = []
    if conditions:
        for col, op, val in conditions:
            cond_objs.append(Condition(col, op, val))

    dr = DataRetrieval(
        table=table_name,
        column=projections or [],
        conditions=cond_objs
    )

    return sm.read_block(dr)

def storage_insert(table_name, row_dict):
    """Memasukkan data ke storage manager.
    Args:
        table_name (str): Nama tabel yang diakses.
        row_dict (dict): Data yang akan dimasukkan dalam bentuk dictionary {kolom: nilai}.
    Returns:
        int: Jumlah data yang telah dimasukkan.
    """
    cols = list(row_dict.keys())
    vals = list(row_dict.values())

    dw = DataWrite(
        table=table_name,
        column=cols,
        new_value=vals,
        conditions=[]
    )

    out = sm.write_block(dw)
    return out


def storage_update(table_name, set_dict, condition_tuple):
    """Memperbarui data di storage manager.
    Args:
        table_name (str): Nama tabel yang diakses.
        set_dict (dict): Data yang akan diperbarui dalam bentuk dictionary {kolom: nilai_baru}.
        condition_tuple (tuple): Kondisi dalam bentuk (kolom, operator, nilai).
    Returns:
        int: Jumlah data yang telah diperbarui.
    """
    col, op, val = condition_tuple
    cond = Condition(col, op, val)

    dw = DataWrite(
        table=table_name,
        column=list(set_dict.keys()),
        new_value=list(set_dict.values()),
        conditions=[cond]
    )

    affected = sm.write_block(dw)
    return affected


def storage_delete(table_name, condition_tuple):
    """Menghapus data dari storage manager.
    Args:
        table_name (str): Nama tabel yang diakses.
        condition_tuple (tuple): Kondisi dalam bentuk (kolom, operator, nilai).
    Returns:
        int: Jumlah data yang telah dihapus.
    """
    col, op, val = condition_tuple
    cond = Condition(col, op, val)

    dd = DataDeletion(
        table=table_name,
        conditions=[cond]
    )

    deleted = sm.delete_block(dd)
    return deleted
