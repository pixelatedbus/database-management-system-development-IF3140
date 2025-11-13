"""Utility functions untuk Storage Manager.

File ini berisi helper functions untuk operasi dasar seperti
evaluasi kondisi, I/O file, dan validasi data.
"""
from __future__ import annotations

import os
import struct
import json
from typing import Any, Dict, List, Tuple
from .models import Condition, ColumnDefinition


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


def validate_value_for_column(value: Any, column_def: ColumnDefinition) -> None:
    """Validasi value sesuai dengan column definition.

    Args:
        value: Nilai yang akan divalidasi
        column_def: Definisi kolom

    Raises:
        ValueError: Jika value tidak valid untuk column tersebut
    """
    # Check NULL constraint
    if value is None:
        if not column_def.is_nullable:
            raise ValueError(f"Column '{column_def.name}' cannot be NULL")
        return  # NULL is valid for nullable columns

    # Check type and constraints
    if column_def.data_type == "INTEGER":
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"Column '{column_def.name}' expects INTEGER, got {type(value).__name__}")

    elif column_def.data_type == "FLOAT":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError(f"Column '{column_def.name}' expects FLOAT, got {type(value).__name__}")

    elif column_def.data_type == "VARCHAR":
        str_value = str(value)
        if len(str_value) > column_def.size:
            raise ValueError(
                f"Column '{column_def.name}' VARCHAR({column_def.size}): "
                f"value too long ({len(str_value)} > {column_def.size})"
            )

    elif column_def.data_type == "CHAR":
        str_value = str(value)
        if len(str_value) > column_def.size:
            raise ValueError(
                f"Column '{column_def.name}' CHAR({column_def.size}): "
                f"value too long ({len(str_value)} > {column_def.size})"
            )


def validate_row_for_schema(row: Dict[str, Any], columns: List[ColumnDefinition]) -> None:
    """Validasi row sesuai dengan schema (column definitions).

    Args:
        row: Row data yang akan divalidasi
        columns: List column definitions

    Raises:
        ValueError: Jika row tidak valid untuk schema
    """
    # Check semua kolom yang required ada
    for col_def in columns:
        if col_def.name not in row:
            # Jika tidak ada value, cek apakah boleh NULL atau punya default
            if not col_def.is_nullable and col_def.default_value is None:
                raise ValueError(f"Column '{col_def.name}' is required but not provided")
        else:
            # Validasi value untuk kolom tersebut
            validate_value_for_column(row[col_def.name], col_def)


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


def calculate_row_size(row: Dict[str, Any], schema: List[str]) -> int:
    """Hitung ukuran byte dari satu row.

    Args:
        row: Row data
        schema: List nama kolom

    Returns:
        Ukuran row dalam bytes
    """
    row_bytes = serialize_row(row, schema)
    return len(row_bytes)


def calculate_average_row_size(rows: List[Dict[str, Any]], schema: List[str]) -> float:
    """Hitung rata-rata ukuran row (l_r).

    Args:
        rows: List of rows
        schema: List nama kolom

    Returns:
        Rata-rata ukuran row dalam bytes
    """
    if not rows:
        return 0.0

    total_size = sum(calculate_row_size(row, schema) for row in rows)
    return total_size / len(rows)


def calculate_blocking_factor(block_size: int, avg_row_size: float) -> int:
    """Hitung blocking factor (f_r) - berapa tuple bisa muat per block.

    Args:
        block_size: Ukuran block dalam bytes
        avg_row_size: Rata-rata ukuran row

    Returns:
        Blocking factor (jumlah tuple per block)
    """
    if avg_row_size == 0:
        return 0

    # Reserve 4 bytes untuk row count per block
    usable_space = block_size - 4
    return int(usable_space / avg_row_size)


def write_binary_table(file_path: str, rows: List[Dict[str, Any]], schema: List[str], block_size: int = 4096) -> None:
    """Tulis tabel ke binary file dengan block-based structure.

    Format file:
    - Header: magic bytes, version, schema, block_size, num_blocks
    - Data: blocks (each block contains row_count + rows)

    Args:
        file_path: Path ke file yang akan ditulis
        rows: List of row dictionaries
        schema: List nama kolom
        block_size: Ukuran maksimal per block (default 4096 bytes)
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

        # 4. Block size
        f.write(struct.pack('<I', block_size))

        # 5. Organize rows into blocks
        blocks = []
        current_block = []
        current_block_size = 4  # start with 4 bytes for row_count

        for row in rows:
            row_bytes = serialize_row(row, schema)
            row_size = len(row_bytes)

            # Check if adding this row would exceed block_size
            if current_block_size + row_size > block_size and current_block:
                # Save current block and start new one
                blocks.append(current_block)
                current_block = []
                current_block_size = 4

            current_block.append(row_bytes)
            current_block_size += row_size

        # Don't forget the last block
        if current_block:
            blocks.append(current_block)

        # 6. Number of blocks
        f.write(struct.pack('<I', len(blocks)))

        # 7. Write each block
        for block in blocks:
            # Row count in this block
            f.write(struct.pack('<I', len(block)))

            # Row data
            for row_bytes in block:
                f.write(row_bytes)


def read_binary_table_streaming(file_path: str, filter_fn=None):
    """Generator yang baca tabel per-block (MEMORY EFFICIENT - streaming).

    ✅ RECOMMENDED for READ operations!

    Benefits:
    - Reads data block-by-block (doesn't load all into memory)
    - Memory usage: ~1 row at a time vs entire table
    - Supports on-the-fly filtering
    - Scalable for large tables (tested with 20k+ rows)

    Use cases:
    - SELECT queries (read-only operations)
    - Filtering/searching data
    - Large tables where memory is a concern

    Args:
        file_path: Path ke file yang akan dibaca
        filter_fn: Optional function(row) -> bool untuk filter rows on-the-fly

    Yields:
        Dict[str, Any]: Row data yang sudah di-filter (jika ada filter_fn)

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

        # 4. Read block size
        block_size = struct.unpack('<I', f.read(4))[0]

        # 5. Read number of blocks
        num_blocks = struct.unpack('<I', f.read(4))[0]

        # 6. Read blocks ONE AT A TIME (streaming)
        for _ in range(num_blocks):
            # Read row count for this block
            row_count_bytes = f.read(4)
            if len(row_count_bytes) < 4:
                break
            row_count = struct.unpack('<I', row_count_bytes)[0]

            # Read all rows in this block
            for _ in range(row_count):
                # Read row length (4 bytes)
                row_length_bytes = f.read(4)
                if len(row_length_bytes) < 4:
                    return
                row_length = struct.unpack('<I', row_length_bytes)[0]

                # Read row data (actual content, without length header)
                row_data = f.read(row_length)
                if len(row_data) < row_length:
                    return

                # Deserialize row (pass full buffer: length + data)
                full_row_buffer = row_length_bytes + row_data
                row, _ = deserialize_row(full_row_buffer, 0, schema)

                # Apply filter if provided
                if filter_fn is None or filter_fn(row):
                    yield row

def append_row_to_table(file_path: str, row: Dict[str, Any], schema: List[str], block_size: int) -> None:
    """Append single row ke binary table tanpa load semua data (optimized).

    Strategy:
    1. Read header untuk tahu struktur file
    2. Navigate ke block terakhir
    3. Check apakah row muat di block terakhir
    4. Jika muat: update row_count block terakhir + append row
    5. Jika tidak muat: buat block baru

    Args:
        file_path: Path ke file
        row: Row data yang akan di-append
        schema: Schema columns
        block_size: Block size limit

    Raises:
        ValueError: Jika file format invalid
    """
    row_bytes = serialize_row(row, schema)
    row_size = len(row_bytes)

    with open(file_path, 'r+b') as f:
        # 1. Read header
        magic = f.read(4)
        if magic != MAGIC_BYTES:
            raise ValueError(f"Invalid file format: expected {MAGIC_BYTES}, got {magic}")

        version = struct.unpack('<I', f.read(4))[0]
        if version != VERSION:
            raise ValueError(f"Unsupported version: {version}")

        # Skip schema
        schema_length = struct.unpack('<I', f.read(4))[0]
        f.seek(schema_length, 1)  # seek relative

        # Skip block_size (4 bytes)
        f.seek(4, 1)

        # Read num_blocks
        num_blocks_pos = f.tell()
        num_blocks = struct.unpack('<I', f.read(4))[0]

        if num_blocks == 0:
            # No blocks yet, create first block
            f.write(struct.pack('<I', 1))  # row count = 1
            f.write(row_bytes)

            # Update num_blocks in header
            f.seek(num_blocks_pos)
            f.write(struct.pack('<I', 1))
            return

        # 2. Navigate to last block
        # Need to iterate through blocks to find the last one
        blocks_start = f.tell()
        last_block_pos = blocks_start

        for i in range(num_blocks):
            block_pos = f.tell()
            row_count = struct.unpack('<I', f.read(4))[0]

            # Calculate block data size by reading each row length
            block_data_size = 0
            for _ in range(row_count):
                row_length = struct.unpack('<I', f.read(4))[0]
                block_data_size += 4 + row_length  # row length header + row data
                f.seek(row_length, 1)  # skip row data

            if i == num_blocks - 1:
                # This is the last block
                last_block_pos = block_pos
                last_block_row_count = row_count
                last_block_size = 4 + block_data_size  # row_count header + data

        # 3. Check if row fits in last block
        if last_block_size + row_size <= block_size:
            # Row fits! Update last block
            # Update row count
            f.seek(last_block_pos)
            f.write(struct.pack('<I', last_block_row_count + 1))

            # Seek to end of file and append row
            f.seek(0, 2)  # SEEK_END
            f.write(row_bytes)
        else:
            # Row doesn't fit, create new block
            f.seek(0, 2)  # SEEK_END
            f.write(struct.pack('<I', 1))  # new block with 1 row
            f.write(row_bytes)

            # Update num_blocks in header
            f.seek(num_blocks_pos)
            f.write(struct.pack('<I', num_blocks + 1))


def append_block_to_table(file_path: str, rows: List[Dict[str, Any]], schema: List[str], block_size: int) -> int:
    """Append multiple rows ke binary table (BATCH INSERT - efficient!).

    Strategy:
    1. Serialize semua rows
    2. Group rows into blocks (sesuai block_size)
    3. Append blocks ke file without loading existing data

    Args:
        file_path: Path ke file
        rows: List of rows yang akan di-append
        schema: Schema columns
        block_size: Block size limit

    Returns:
        Jumlah rows yang berhasil di-insert

    Raises:
        ValueError: Jika file format invalid
    """
    if not rows:
        return 0

    # Jika file belum ada, create dengan write_binary_table
    if not os.path.exists(file_path):
        write_binary_table(file_path, rows, schema, block_size)
        return len(rows)

    with open(file_path, 'r+b') as f:
        # 1. Read header
        magic = f.read(4)
        if magic != MAGIC_BYTES:
            raise ValueError(f"Invalid file format: expected {MAGIC_BYTES}, got {magic}")

        version = struct.unpack('<I', f.read(4))[0]
        if version != VERSION:
            raise ValueError(f"Unsupported version: {version}")

        # Skip schema
        schema_length = struct.unpack('<I', f.read(4))[0]
        f.seek(schema_length, 1)

        # Skip block_size
        f.seek(4, 1)

        # Read num_blocks
        num_blocks_pos = f.tell()
        num_blocks = struct.unpack('<I', f.read(4))[0]

        # 2. Navigate to last block to check if we can merge
        last_block_pos = None
        last_block_row_count = 0
        last_block_size = 0

        if num_blocks > 0:
            # Navigate through all blocks to find the last one
            for i in range(num_blocks):
                block_pos = f.tell()
                row_count = struct.unpack('<I', f.read(4))[0]

                block_data_size = 0
                for _ in range(row_count):
                    row_length = struct.unpack('<I', f.read(4))[0]
                    block_data_size += 4 + row_length
                    f.seek(row_length, 1)

                if i == num_blocks - 1:
                    last_block_pos = block_pos
                    last_block_row_count = row_count
                    last_block_size = 4 + block_data_size

        # 3. Try to fit rows into last block first, then create new blocks
        rows_to_insert = list(rows)  # copy
        blocks_added = 0

        # Try to merge with last block
        if last_block_pos is not None:
            while rows_to_insert:
                row_bytes = serialize_row(rows_to_insert[0], schema)
                row_size = len(row_bytes)

                if last_block_size + row_size <= block_size:
                    # Fits in last block! Append it
                    f.seek(0, 2)  # SEEK_END
                    f.write(row_bytes)

                    # Update last block row count
                    f.seek(last_block_pos)
                    last_block_row_count += 1
                    f.write(struct.pack('<I', last_block_row_count))

                    last_block_size += row_size
                    rows_to_insert.pop(0)
                else:
                    # Can't fit anymore in last block
                    break

        # 4. Create new blocks for remaining rows
        if rows_to_insert:
            f.seek(0, 2)  # SEEK_END

            current_block = []
            current_block_size = 4  # row_count header

            for row in rows_to_insert:
                row_bytes = serialize_row(row, schema)
                row_size = len(row_bytes)

                # Check if row fits in current block
                if current_block_size + row_size > block_size and current_block:
                    # Write current block
                    f.write(struct.pack('<I', len(current_block)))
                    for rb in current_block:
                        f.write(rb)

                    blocks_added += 1
                    current_block = []
                    current_block_size = 4

                current_block.append(row_bytes)
                current_block_size += row_size

            # Write last block
            if current_block:
                f.write(struct.pack('<I', len(current_block)))
                for rb in current_block:
                    f.write(rb)
                blocks_added += 1

        # 5. Update num_blocks in header
        if blocks_added > 0:
            f.seek(num_blocks_pos)
            f.write(struct.pack('<I', num_blocks + blocks_added))

    return len(rows)
