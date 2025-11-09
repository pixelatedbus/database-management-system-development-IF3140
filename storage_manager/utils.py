"""Utility functions untuk Storage Manager.

File ini berisi helper functions untuk operasi dasar seperti
evaluasi kondisi, I/O file, dan validasi data.
"""
from __future__ import annotations

import struct
import json
from typing import Any, Dict, List, Tuple
from .models import Condition


def evaluate_condition(row: Dict[str, Any], condition: Condition) -> bool:
    """Evaluasi apakah row memenuhi kondisi tertentu.
    
    Args:
        row: Baris data (dictionary dengan column_name -> value)
        condition: Kondisi yang dievaluasi
        
    Returns:
        True jika row memenuhi kondisi, False sebaliknya
        
    Raises:
        ValueError: Jika operator tidak dikenali
    """
    if condition.column not in row:
        return False
    
    row_value = row[condition.column]
    condition_value = condition.operand

    # evaluasi berdasarkan operator
    if condition.operation == "=":
        return row_value == condition_value
    elif condition.operation == "<>":
        return row_value != condition_value
    elif condition.operation == "<":
        return row_value < condition_value
    elif condition.operation == "<=":
        return row_value <= condition_value
    elif condition.operation == ">":
        return row_value > condition_value
    elif condition.operation == ">=":
        return row_value >= condition_value
    else:
        raise ValueError(f"Operator tidak dikenali: {condition.operation}")


def project_columns(row: Dict[str, Any], columns: list[str]) -> Dict[str, Any]:
    """Proyeksi baris untuk hanya memuat kolom tertentu.
    
    Args:
        row: Baris data lengkap
        columns: Daftar kolom yang ingin diambil
        
    Returns:
        Dictionary baru dengan hanya kolom yang diminta
    """
    projected = {}
    for col in columns:
        if col in row:
            projected[col] = row[col]
    return projected


def validate_table_name(table_name: str) -> bool:
    """Validasi format nama tabel.

    Args:
        table_name: Nama tabel yang akan divalidasi

    Returns:
        True jika valid, False sebaliknya
    """
    if not table_name or not isinstance(table_name, str):
        return False
    if not table_name.replace('_', '').isalnum():
        return False
    if table_name[0].isdigit():
        return False
    return True


# ========== Binary File I/O Functions ==========

MAGIC_BYTES = b'SMDB'
VERSION = 1

def serialize_value(value: Any) -> bytes:
    """Serialisasi satu value ke binary format.

    Format (little-endian, no padding):
    - 1 byte type indicator:
        0 = None
        1 = int
        2 = float
        3 = str
        4 = bool
    - N bytes data (tergantung tipe)

    Args:
        value: Value yang akan diserialisasi

    Returns:
        Binary representation dari value
    """
    if value is None:
        return struct.pack('<B', 0)  # type indicator: None

    elif isinstance(value, bool):  # harus sebelum int karena bool subclass dari int
        return struct.pack('<B?', 4, value)  # type indicator: bool + 1 byte boolean

    elif isinstance(value, int):
        return struct.pack('<Bq', 1, value)  # type indicator: int + 8 bytes signed long

    elif isinstance(value, float):
        return struct.pack('<Bd', 2, value)  # type indicator: float + 8 bytes double

    elif isinstance(value, str):
        encoded = value.encode('utf-8')
        length = len(encoded)
        # Pack type indicator and length first, then append the string bytes
        return struct.pack('<BI', 3, length) + encoded

    else:
        # fallback: convert to string
        encoded = str(value).encode('utf-8')
        length = len(encoded)
        return struct.pack('<BI', 3, length) + encoded


def deserialize_value(data: bytes, offset: int) -> Tuple[Any, int]:
    """Deserialisasi satu value dari binary format.

    Args:
        data: Binary data buffer
        offset: Posisi awal dalam buffer

    Returns:
        Tuple (value, new_offset) dimana new_offset adalah posisi setelah membaca
    """
    type_indicator = struct.unpack_from('<B', data, offset)[0]
    offset += 1

    if type_indicator == 0:  # None
        return None, offset

    elif type_indicator == 1:  # int
        value = struct.unpack_from('<q', data, offset)[0]
        return value, offset + 8

    elif type_indicator == 2:  # float
        value = struct.unpack_from('<d', data, offset)[0]
        return value, offset + 8

    elif type_indicator == 3:  # str
        length = struct.unpack_from('<I', data, offset)[0]
        offset += 4
        # Read the string bytes directly instead of using struct.unpack
        value = data[offset:offset + length].decode('utf-8')
        return value, offset + length

    elif type_indicator == 4:  # bool
        value = struct.unpack_from('<?', data, offset)[0]
        return value, offset + 1

    else:
        raise ValueError(f"Unknown type indicator: {type_indicator}")


def serialize_row(row: Dict[str, Any], schema: List[str]) -> bytes:
    """Serialisasi satu row ke binary format.

    Args:
        row: Dictionary berisi data row
        schema: List nama kolom (urutan penting!)

    Returns:
        Binary representation dari row
    """
    row_data = b''

    # serialisasi tiap kolom sesuai urutan schema
    for column_name in schema:
        value = row.get(column_name, None)
        row_data += serialize_value(value)

    # tambahkan row length di depan untuk memudahkan parsing
    row_length = len(row_data)
    return struct.pack('<I', row_length) + row_data


def deserialize_row(data: bytes, offset: int, schema: List[str]) -> Tuple[Dict[str, Any], int]:
    """Deserialisasi satu row dari binary format.

    Args:
        data: Binary data buffer
        offset: Posisi awal row dalam buffer
        schema: List nama kolom (urutan penting!)

    Returns:
        Tuple (row_dict, new_offset)
    """
    # skip row length (4 bytes) - sudah disimpan untuk konsistensi format
    offset += 4

    row = {}
    for column_name in schema:
        value, offset = deserialize_value(data, offset)
        row[column_name] = value

    return row, offset


def write_binary_table(file_path: str, rows: List[Dict[str, Any]], schema: List[str]) -> None:
    """Tulis tabel ke binary file.

    Format file:
    - Header: magic bytes, version, schema, row count
    - Data: serialized rows

    Args:
        file_path: Path ke file yang akan ditulis
        rows: List of row dictionaries
        schema: List nama kolom
    """
    with open(file_path, 'wb') as f:
        # 1. Magic bytes
        f.write(MAGIC_BYTES)

        # 2. Version
        f.write(struct.pack('<I', VERSION))

        # 3. Schema (as JSON string)
        schema_json = json.dumps(schema).encode('utf-8')
        schema_length = len(schema_json)
        f.write(struct.pack('<I', schema_length))
        f.write(schema_json)

        # 4. Number of rows
        f.write(struct.pack('<I', len(rows)))

        # 5. Row data
        for row in rows:
            row_bytes = serialize_row(row, schema)
            f.write(row_bytes)


def read_binary_table(file_path: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Baca tabel dari binary file.

    Args:
        file_path: Path ke file yang akan dibaca

    Returns:
        Tuple (rows, schema)

    Raises:
        ValueError: Jika format file tidak valid
    """
    with open(file_path, 'rb') as f:
        # 1. Verify magic bytes
        magic = f.read(4)
        if magic != MAGIC_BYTES:
            raise ValueError(f"Invalid file format. Expected {MAGIC_BYTES}, got {magic}")

        # 2. Read version
        version = struct.unpack('<I', f.read(4))[0]
        if version != VERSION:
            raise ValueError(f"Unsupported version: {version}")

        # 3. Read schema
        schema_length = struct.unpack('<I', f.read(4))[0]
        schema_json = f.read(schema_length).decode('utf-8')
        schema = json.loads(schema_json)

        # 4. Read number of rows
        num_rows = struct.unpack('<I', f.read(4))[0]

        # 5. Read all rows
        remaining_data = f.read()
        offset = 0
        rows = []

        for _ in range(num_rows):
            row, offset = deserialize_row(remaining_data, offset, schema)
            rows.append(row)

        return rows, schema
