# storage manager implementation
# operasi read, write, delete, index, dan statistik
from __future__ import annotations

import os
import struct
from typing import Any, Dict, List, Optional, Union, Tuple

from .models import (
    Condition,
    DataRetrieval,
    DataWrite,
    DataDeletion,
    Statistic,
    ColumnDefinition,
    ForeignKey
)
from .utils import (
    evaluate_condition,
    project_columns,
    validate_table_name,
    validate_row_for_schema,
    validate_value_for_column,
    read_binary_table_streaming,
    write_binary_table,
    append_row_to_table,
    append_block_to_table
)


class StorageManager:
    """komponen storage manager untuk handle penyimpanan data.

    tanggung jawab:
    - simpan data dalam binary file di harddisk
    - baca, tulis, dan hapus blok data
    - kelola indeks (b+ tree atau hash)
    - sediakan statistik untuk optimisasi query

    implementasi:
    - 1 file per tabel (format: data/table_name.dat)
    - data disimpan dalam format binary
    - setiap tabel punya metadata schema yang disimpan terpisah
    """

    def __init__(self, data_dir: str = "data", block_size: int = 4096):
        """inisialisasi storage manager.

        args:
            data_dir: direktori tempat simpan file data tabel (default: 'data')
            block_size: ukuran blok dalam bytes (default: 4096 bytes)
        """
        self.data_dir = data_dir
        self.block_size = block_size

        # struktur manajemen file
        self.tables: Dict[str, Dict[str, Any]] = {}  # nama_tabel -> schema
        self.stats: Dict[str, Statistic] = {}  # nama_tabel -> statistic
        self.indexes: Dict[tuple, Any] = {}  # (table_name, column_name) -> index structure
        
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        
        self._load_table_schemas()
        print(f"storage manager diinisialisasi di: {os.path.abspath(self.data_dir)}")

    # ========== file management ==========

    def _load_table_schemas(self) -> None:
        """load schema dari semua tabel yang ada di data_dir."""
        metadata_file = self._get_metadata_file_path()
        if os.path.exists(metadata_file):
            try:
                self.tables = self._read_binary_metadata(metadata_file)
                print(f"✓ loaded {len(self.tables)} tabel dari metadata")
            except Exception as e:
                print(f"⚠ error loading metadata: {e}")
                self.tables = {}

    def _save_table_schemas(self) -> None:
        """simpan schema dari semua tabel ke metadata file."""
        metadata_file = self._get_metadata_file_path()
        try:
            self._write_binary_metadata(metadata_file, self.tables)
        except Exception as e:
            print(f"⚠ error saving metadata: {e}")

    def _get_metadata_file_path(self) -> str:
        """dapetin path file metadata."""
        return os.path.join(self.data_dir, "__metadata__.dat")

    def _get_table_file_path(self, table_name: str) -> str:
        """dapetin path file untuk tabel tertentu."""
        return os.path.join(self.data_dir, f"{table_name}.dat")

    def _write_binary_metadata(self, file_path: str, tables: Dict[str, Dict[str, Any]]) -> None:
        """tulis metadata ke binary file.

        format file:
        - magic bytes: b'META'
        - version: 4 bytes
        - jumlah tabel: 4 bytes
        - untuk tiap tabel:
          - panjang nama tabel + nama tabel
          - metadata tabel (columns, primary_keys, foreign_keys)
        """
        with open(file_path, 'wb') as f:
            # magic bytes
            f.write(b'META')

            # version
            f.write(struct.pack('<I', 1))

            # jumlah tabel
            f.write(struct.pack('<I', len(tables)))

            # tiap tabel
            for table_name, table_meta in tables.items():
                # nama tabel
                table_name_bytes = table_name.encode('utf-8')
                f.write(struct.pack('<I', len(table_name_bytes)))
                f.write(table_name_bytes)

                # columns
                columns = table_meta.get('columns', [])
                f.write(struct.pack('<I', len(columns)))
                for col in columns:
                    # nama kolom
                    col_name = col['name'].encode('utf-8')
                    f.write(struct.pack('<I', len(col_name)))
                    f.write(col_name)

                    # data_type
                    data_type = col['data_type'].encode('utf-8')
                    f.write(struct.pack('<I', len(data_type)))
                    f.write(data_type)

                    # size (bisa None)
                    if col['size'] is None:
                        f.write(struct.pack('<B?', 0, False))  # null marker
                    else:
                        f.write(struct.pack('<B?I', 1, True, col['size']))  # not null + value

                    # is_primary_key
                    f.write(struct.pack('<?', col['is_primary_key']))

                    # is_nullable
                    f.write(struct.pack('<?', col['is_nullable']))

                    # default_value (bisa None atau value)
                    default_val = col.get('default_value')
                    if default_val is None:
                        f.write(struct.pack('<B', 0))  # null
                    elif isinstance(default_val, bool):
                        f.write(struct.pack('<B?', 4, default_val))
                    elif isinstance(default_val, int):
                        f.write(struct.pack('<Bq', 1, default_val))
                    elif isinstance(default_val, float):
                        f.write(struct.pack('<Bd', 2, default_val))
                    elif isinstance(default_val, str):
                        encoded = default_val.encode('utf-8')
                        f.write(struct.pack('<BI', 3, len(encoded)))
                        f.write(encoded)
                    else:
                        f.write(struct.pack('<B', 0))  # fallback ke null

                # primary_keys
                pk_list = table_meta.get('primary_keys', [])
                f.write(struct.pack('<I', len(pk_list)))
                for pk in pk_list:
                    pk_bytes = pk.encode('utf-8')
                    f.write(struct.pack('<I', len(pk_bytes)))
                    f.write(pk_bytes)

                # foreign_keys
                fk_list = table_meta.get('foreign_keys', [])
                f.write(struct.pack('<I', len(fk_list)))
                for fk in fk_list:
                    # column
                    col_bytes = fk['column'].encode('utf-8')
                    f.write(struct.pack('<I', len(col_bytes)))
                    f.write(col_bytes)

                    # references_table
                    ref_table = fk['references_table'].encode('utf-8')
                    f.write(struct.pack('<I', len(ref_table)))
                    f.write(ref_table)

                    # references_column
                    ref_col = fk['references_column'].encode('utf-8')
                    f.write(struct.pack('<I', len(ref_col)))
                    f.write(ref_col)

                    # on_delete
                    on_del = fk['on_delete'].encode('utf-8')
                    f.write(struct.pack('<I', len(on_del)))
                    f.write(on_del)

                    # on_update
                    on_upd = fk['on_update'].encode('utf-8')
                    f.write(struct.pack('<I', len(on_upd)))
                    f.write(on_upd)

    def _read_binary_metadata(self, file_path: str) -> Dict[str, Dict[str, Any]]:
        """baca metadata dari binary file."""
        with open(file_path, 'rb') as f:
            # verify magic bytes
            magic = f.read(4)
            if magic != b'META':
                raise ValueError(f"invalid metadata format: expected b'META', got {magic}")

            # version
            version = struct.unpack('<I', f.read(4))[0]
            if version != 1:
                raise ValueError(f"unsupported metadata version: {version}")

            # jumlah tabel
            num_tables = struct.unpack('<I', f.read(4))[0]

            tables = {}
            for _ in range(num_tables):
                # nama tabel
                table_name_len = struct.unpack('<I', f.read(4))[0]
                table_name = f.read(table_name_len).decode('utf-8')

                # columns
                num_cols = struct.unpack('<I', f.read(4))[0]
                columns = []
                for _ in range(num_cols):
                    col = {}

                    # nama kolom
                    col_name_len = struct.unpack('<I', f.read(4))[0]
                    col['name'] = f.read(col_name_len).decode('utf-8')

                    # data_type
                    data_type_len = struct.unpack('<I', f.read(4))[0]
                    col['data_type'] = f.read(data_type_len).decode('utf-8')

                    # size
                    _ = struct.unpack('<B', f.read(1))[0]  # skip marker
                    has_size = struct.unpack('<?', f.read(1))[0]
                    if has_size:
                        col['size'] = struct.unpack('<I', f.read(4))[0]
                    else:
                        col['size'] = None

                    # is_primary_key
                    col['is_primary_key'] = struct.unpack('<?', f.read(1))[0]

                    # is_nullable
                    col['is_nullable'] = struct.unpack('<?', f.read(1))[0]

                    # default_value
                    default_type = struct.unpack('<B', f.read(1))[0]
                    if default_type == 0:  # null
                        col['default_value'] = None
                    elif default_type == 1:  # int
                        col['default_value'] = struct.unpack('<q', f.read(8))[0]
                    elif default_type == 2:  # float
                        col['default_value'] = struct.unpack('<d', f.read(8))[0]
                    elif default_type == 3:  # str
                        str_len = struct.unpack('<I', f.read(4))[0]
                        col['default_value'] = f.read(str_len).decode('utf-8')
                    elif default_type == 4:  # bool
                        col['default_value'] = struct.unpack('<?', f.read(1))[0]
                    else:
                        col['default_value'] = None

                    columns.append(col)

                # primary_keys
                num_pks = struct.unpack('<I', f.read(4))[0]
                primary_keys = []
                for _ in range(num_pks):
                    pk_len = struct.unpack('<I', f.read(4))[0]
                    primary_keys.append(f.read(pk_len).decode('utf-8'))

                # foreign_keys
                num_fks = struct.unpack('<I', f.read(4))[0]
                foreign_keys = []
                for _ in range(num_fks):
                    fk = {}

                    # column
                    col_len = struct.unpack('<I', f.read(4))[0]
                    fk['column'] = f.read(col_len).decode('utf-8')

                    # references_table
                    ref_table_len = struct.unpack('<I', f.read(4))[0]
                    fk['references_table'] = f.read(ref_table_len).decode('utf-8')

                    # references_column
                    ref_col_len = struct.unpack('<I', f.read(4))[0]
                    fk['references_column'] = f.read(ref_col_len).decode('utf-8')

                    # on_delete
                    on_del_len = struct.unpack('<I', f.read(4))[0]
                    fk['on_delete'] = f.read(on_del_len).decode('utf-8')

                    # on_update
                    on_upd_len = struct.unpack('<I', f.read(4))[0]
                    fk['on_update'] = f.read(on_upd_len).decode('utf-8')

                    foreign_keys.append(fk)

                tables[table_name] = {
                    'columns': columns,
                    'primary_keys': primary_keys,
                    'foreign_keys': foreign_keys
                }

            return tables


    # ========== table management ==========

    def create_table(
        self,
        table_name: str,
        columns: Union[List[str], List[ColumnDefinition]],
        primary_keys: Optional[List[str]] = None,
        foreign_keys: Optional[List[ForeignKey]] = None
    ) -> None:
        """bikin tabel baru dengan schema dan constraints.

        args:
            table_name: nama tabel
            columns: list nama kolom (backward compatible) atau list columndefinition
            primary_keys: list nama kolom yang jadi primary key (opsional)
            foreign_keys: list foreignkey constraints (opsional)

        raises:
            valueerror: jika nama tabel invalid atau sudah ada

        example:
            # cara 1: backward compatible (hanya nama kolom)
            sm.create_table("users", ["id", "name", "email"])

            # cara 2: dengan tipe data lengkap
            sm.create_table(
                "users",
                columns=[
                    ColumnDefinition("id", "INTEGER", is_primary_key=True),
                    ColumnDefinition("name", "VARCHAR", size=50),
                    ColumnDefinition("email", "VARCHAR", size=100),
                ],
                primary_keys=["id"]
            )
        """
        if not validate_table_name(table_name):
            raise ValueError(f"Nama tabel tidak valid: {table_name}")

        if table_name in self.tables:
            raise ValueError(f"Tabel '{table_name}' sudah ada")

        # handle backward compatibility: jika columns adalah list[str]
        if columns and isinstance(columns[0], str):
            # convert ke columndefinition dengan tipe default (varchar 255)
            column_defs = [
                ColumnDefinition(name=col, data_type="VARCHAR", size=255, is_nullable=True)
                for col in columns
            ]
        else:
            column_defs = columns

        # validasi primary keys
        if primary_keys:
            for pk in primary_keys:
                # cari column definition untuk pk
                pk_col = next((c for c in column_defs if c.name == pk), None)
                if pk_col is None:
                    raise ValueError(f"Primary key '{pk}' tidak ada di columns")
                # set sebagai primary key
                pk_col.is_primary_key = True
                pk_col.is_nullable = False

        # validasi foreign keys
        if foreign_keys:
            for fk in foreign_keys:
                # cek column exists
                fk_col = next((c for c in column_defs if c.name == fk.column), None)
                if fk_col is None:
                    raise ValueError(f"Foreign key column '{fk.column}' tidak ada")

                # cek referenced table exists
                if fk.references_table not in self.tables:
                    raise ValueError(f"Referenced table '{fk.references_table}' tidak ditemukan")

        # simpan metadata dengan format lengkap
        self.tables[table_name] = {
            "columns": [self._column_def_to_dict(c) for c in column_defs],
            "primary_keys": primary_keys or [],
            "foreign_keys": [self._foreign_key_to_dict(fk) for fk in foreign_keys] if foreign_keys else []
        }
        self._save_table_schemas()

        # buat file binary kosong (gunakan nama kolom saja untuk compatibility)
        schema_names = [c.name for c in column_defs]
        table_file = self._get_table_file_path(table_name)
        write_binary_table(table_file, [], schema_names, self.block_size)

        print(f"✓ tabel '{table_name}' berhasil dibuat dengan {len(column_defs)} kolom")

    def _column_def_to_dict(self, col: ColumnDefinition) -> Dict[str, Any]:
        """convert columndefinition ke dictionary untuk json storage."""
        return {
            "name": col.name,
            "data_type": col.data_type,
            "size": col.size,
            "is_primary_key": col.is_primary_key,
            "is_nullable": col.is_nullable,
            "default_value": col.default_value
        }

    def _dict_to_column_def(self, data: Dict[str, Any]) -> ColumnDefinition:
        """convert dictionary ke columndefinition."""
        return ColumnDefinition(
            name=data["name"],
            data_type=data["data_type"],
            size=data.get("size"),
            is_primary_key=data.get("is_primary_key", False),
            is_nullable=data.get("is_nullable", True),
            default_value=data.get("default_value")
        )

    def _foreign_key_to_dict(self, fk: ForeignKey) -> Dict[str, Any]:
        """convert foreignkey ke dictionary untuk json storage."""
        return {
            "column": fk.column,
            "references_table": fk.references_table,
            "references_column": fk.references_column,
            "on_delete": fk.on_delete,
            "on_update": fk.on_update
        }

    def _get_column_definitions(self, table_name: str) -> List[ColumnDefinition]:
        """ambil column definitions untuk tabel tertentu."""
        if table_name not in self.tables:
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan")

        table_meta = self.tables[table_name]
        return [self._dict_to_column_def(c) for c in table_meta["columns"]]

    def insert_rows(self, table_name: str, rows: List[Dict[str, Any]], validate: bool = True) -> None:
        """insert rows ke tabel dengan validasi tipe data (OPTIMIZED - batch insert).

        args:
            table_name: nama tabel
            rows: list of row dictionaries
            validate: apakah melakukan validasi tipe data (default: true)

        raises:
            valueerror: jika tabel tidak ditemukan atau data tidak valid
        """
        if table_name not in self.tables:
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan")

        # ambil column definitions
        column_defs = self._get_column_definitions(table_name)

        # aplikasikan default values untuk kolom yang tidak diisi
        processed_rows = []
        for row in rows:
            processed_row = row.copy()
            for col_def in column_defs:
                if col_def.name not in processed_row:
                    if col_def.default_value is not None:
                        processed_row[col_def.name] = col_def.default_value
                    elif col_def.is_nullable:
                        processed_row[col_def.name] = None
            processed_rows.append(processed_row)

        # validasi setiap row jika diminta
        if validate:
            for i, row in enumerate(processed_rows):
                try:
                    validate_row_for_schema(row, column_defs)
                except ValueError as e:
                    raise ValueError(f"Row {i+1} validation failed: {e}")

        # ambil schema names untuk binary file
        table_meta = self.tables[table_name]
        schema_names = [c["name"] for c in table_meta["columns"]]

        table_file = self._get_table_file_path(table_name)

        # OPTIMIZED: gunakan append_block_to_table untuk batch insert tanpa load semua data!
        if not os.path.exists(table_file):
            write_binary_table(table_file, processed_rows, schema_names, self.block_size)
        else:
            append_block_to_table(table_file, processed_rows, schema_names, self.block_size)

        print(f"✓ inserted {len(rows)} rows ke tabel '{table_name}' (optimized batch insert)")

    # ========== core operations ==========

    def read_block(self, data_retrieval: DataRetrieval) -> List[Dict[str, Any]]:
        """baca data dari disk berdasarkan parameter retrieval (OPTIMIZED - streaming).

        proses:
        1. validasi tabel ada di storage
        2. stream data per-block dari binary file (ga load semua!)
        3. filter row berdasarkan kondisi (and logic) on-the-fly
        4. proyeksi kolom jika diminta
        5. return hasil

        args:
            data_retrieval: object yang berisi nama tabel, kolom, dan kondisi

        returns:
            list dari baris (setiap baris sebagai dictionary) yang sesuai kondisi

        raises:
            valueerror: jika tabel tidak ditemukan
        """
        table_name = data_retrieval.table

        # 1. validasi tabel ada
        if table_name not in self.tables:
            available = list(self.tables.keys()) if self.tables else "tidak ada"
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan. Tersedia: {available}")

        table_file = self._get_table_file_path(table_name)

        # 2. cek file exists
        if not os.path.exists(table_file):
            print(f"⚠ file tabel '{table_name}' tidak ditemukan")
            return []

        # 3. stream data dari binary file (memory efficient!)
        try:
            # Define filter function untuk streaming
            def row_filter(row):
                return self._row_matches_all_conditions(row, data_retrieval.conditions)

            # Stream rows yang match conditions
            filtered_rows = list(read_binary_table_streaming(table_file, filter_fn=row_filter))
            print(f"✓ found {len(filtered_rows)} matching rows dari tabel '{table_name}' (streamed)")
        except Exception as e:
            raise ValueError(f"error membaca binary file '{table_name}.dat': {e}")

        # 4. proyeksi kolom jika ada
        if data_retrieval.column and len(data_retrieval.column) > 0:
            return [project_columns(row, data_retrieval.column) for row in filtered_rows]
        else:
            return filtered_rows

    def _row_matches_all_conditions(self, row: Dict[str, Any], conditions: List[Condition]) -> bool:
        """cek apakah row memenuhi semua kondisi (and logic).

        args:
            row: baris data (dictionary)
            conditions: list kondisi yang harus dipenuhi

        returns:
            true jika row memenuhi semua kondisi, false sebaliknya
        """
        for condition in conditions:
            if not evaluate_condition(row, condition):
                return False
        return True


    # ========== helpers untuk delete_block ==========

    def _load_table_rows(self, table_name: str) -> List[Dict[str, Any]]:
        """load semua baris dari file tabel (OPTIMIZED - streaming read).

        args:
            table_name: nama tabel

        returns:
            list dari baris (setiap baris sebagai dictionary)

        raises:
            exception: jika gagal membaca binary file
        """
        table_file = self._get_table_file_path(table_name)
        if not os.path.exists(table_file):
            return []

        # OPTIMIZED: gunakan streaming untuk read, lalu collect ke list
        rows = list(read_binary_table_streaming(table_file))
        return rows

    def _save_table_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        """tulis kembali semua baris ke file tabel menggunakan schema yang tersimpan.

        args:
            table_name: nama tabel
            rows: list dari baris (setiap baris sebagai dictionary)

        raises:
            exception: jika penulisan gagal
        """
        schema_names = [c["name"] for c in self.tables[table_name]["columns"]]
        table_file = self._get_table_file_path(table_name)
        write_binary_table(table_file, rows, schema_names, self.block_size)

    # ========== helpers untuk write_block ==========

    def _load_all_rows_with_schema(self, table_name: str) -> Tuple[List[Dict[str, Any]], List[str]]:
        """muat semua baris data dan nama schema (kolom) dari file biner (OPTIMIZED - streaming).

        returns:
            tuple (list of row dictionaries, list of schema names)
        """
        table_file = self._get_table_file_path(table_name)

        # ambil schema names dari metadata
        schema_names = [col.name for col in self._get_column_definitions(table_name)]

        # jika file belum ada, anggap tabel kosong
        if not os.path.exists(table_file):
            return [], schema_names

        # OPTIMIZED: gunakan streaming untuk read, lalu collect ke list
        rows = list(read_binary_table_streaming(table_file))
        return rows, schema_names


    def _apply_defaults_and_validate(self, rows: List[Dict[str, Any]], column_defs: List[ColumnDefinition]) -> None:
        """isi nilai default dan validasi schema pada baris yang diberikan (in-place).

        digunakan saat insert untuk memastikan semua kolom wajib ada.
        """
        schema_names = [col.name for col in column_defs]

        for row in rows:
            # 1. aplikasikan nilai default dan tangani missing column
            for col_def in column_defs:
                col_name = col_def.name

                # jika kolom tidak ada di row yang di-insert:
                if col_name not in row:
                    if col_def.default_value is not None:
                        # isi dengan nilai default
                        row[col_name] = col_def.default_value
                    elif col_def.is_nullable:
                        # boleh null, isi dengan none (atau biarkan saja, tapi di sini kita isi none eksplisit)
                        row[col_name] = None
                    else:
                        # tidak ada nilai, tidak boleh null, dan tidak ada default. ini adalah error.
                        raise ValueError(f"Kolom wajib '{col_name}' tidak disediakan dan tidak memiliki nilai default.")

            # 2. lakukan validasi penuh
            # memanggil fungsi utilitas yang memastikan tipe data, nullability, dan size
            validate_row_for_schema(row, column_defs)


    # ========== write / delete / index / stats ==========
    def write_block(self, data_write: DataWrite) -> int:
            """tulis atau modifikasi data di disk.

            args:
                data_write: object yang berisi tabel, kolom, kondisi, dan nilai baru

            returns:
                jumlah baris yang terpengaruh

            implementasi logika insert/update:
            - insert (tanpa kondisi): append row baru tanpa load semua data
            - update (dengan kondisi): load semua data, scan untuk match, lalu update
            """

            table_name = data_write.table

            if table_name not in self.tables:
                raise ValueError(f"Tabel '{table_name}' tidak ditemukan")

            table_file = self._get_table_file_path(table_name)
            column_defs = self._get_column_definitions(table_name)
            schema_names = [c.name for c in column_defs]

            # ========== logika insert ==========
            if not data_write.conditions:
                # validasi input
                if not data_write.column or not data_write.new_value:
                    raise ValueError("Untuk INSERT, column dan new_value harus diisi")

                # Detect: BATCH INSERT atau SINGLE INSERT?
                # Batch insert: new_value = [[val1, val2], [val1, val2], ...]
                # Single insert: new_value = [val1, val2]
                is_batch = isinstance(data_write.new_value[0], (list, tuple)) if data_write.new_value else False

                if is_batch:
                    # ========== BATCH INSERT (EFFICIENT!) ==========
                    rows_to_insert = []
                    for row_values in data_write.new_value:
                        if len(data_write.column) != len(row_values):
                            raise ValueError(f"Jumlah kolom ({len(data_write.column)}) dan nilai ({len(row_values)}) harus sama")

                        row_data = dict(zip(data_write.column, row_values))
                        rows_to_insert.append(row_data)

                    # Validasi semua rows
                    self._apply_defaults_and_validate(rows_to_insert, column_defs)

                    # Batch insert dengan append_block_to_table (SUPER EFFICIENT!)
                    if not os.path.exists(table_file):
                        write_binary_table(table_file, rows_to_insert, schema_names, self.block_size)
                    else:
                        append_block_to_table(table_file, rows_to_insert, schema_names, self.block_size)

                    print(f"✓ BATCH inserted {len(rows_to_insert)} rows ke tabel '{table_name}' (efficient!)")
                    return len(rows_to_insert)

                else:
                    # ========== SINGLE INSERT ==========
                    if len(data_write.column) != len(data_write.new_value):
                        raise ValueError("Jumlah kolom dan nilai baru harus sama")

                    # buat row baru
                    new_row_data = dict(zip(data_write.column, data_write.new_value))

                    # aplikasikan nilai default dan validasi
                    self._apply_defaults_and_validate([new_row_data], column_defs)

                    # file baru atau file exists?
                    if not os.path.exists(table_file):
                        # file baru - write biasa
                        write_binary_table(table_file, [new_row_data], schema_names, self.block_size)
                    else:
                        # file exists - optimized append tanpa load semua data
                        append_row_to_table(table_file, new_row_data, schema_names, self.block_size)

                    print(f"inserted 1 row ke tabel '{table_name}'")
                    return 1

            # ========== logika update (dengan kondisi) ==========
            else:
                # validasi input
                if not data_write.column or not data_write.new_value:
                    raise ValueError("untuk update, column dan new_value harus diisi")

                if len(data_write.column) != len(data_write.new_value):
                    raise ValueError("jumlah kolom dan nilai baru harus sama")

                update_data = dict(zip(data_write.column, data_write.new_value))

                # pra-validasi tipe data untuk nilai yang di-update
                for col_name, new_val in update_data.items():
                    col_def = next((c for c in column_defs if c.name == col_name), None)
                    if col_def is None:
                        raise ValueError(f"kolom '{col_name}' tidak ada di tabel '{table_name}'")

                    # validasi nilai baru
                    try:
                        validate_value_for_column(new_val, col_def)
                    except ValueError as e:
                        raise ValueError(f"update value validation failed for column '{col_name}': {e}")

                # hanya di update: load semua rows untuk scan kondisi
                all_rows, _ = self._load_all_rows_with_schema(table_name)

                # scan dan update rows yang match
                new_rows = []
                rows_affected = 0

                for row in all_rows:
                    if self._row_matches_all_conditions(row, data_write.conditions):
                        # update row yang match kondisi
                        updated_row = row.copy()
                        for col_name, new_val in update_data.items():
                            updated_row[col_name] = new_val
                        new_rows.append(updated_row)
                        rows_affected += 1
                    else:
                        # keep row yang tidak match
                        new_rows.append(row)

                # write kembali semua rows
                if rows_affected > 0:
                    write_binary_table(table_file, new_rows, schema_names, self.block_size)

                print(f"updated {rows_affected} rows di tabel '{table_name}'")
                return rows_affected



    def delete_block(self, data_deletion: DataDeletion) -> int:
        """hapus data dari disk.

        proses:
        - validasi tabel ada
        - load semua baris
        - tentukan baris yang harus dihapus berdasarkan kondisi (and)
        - simpan ulang baris yang tersisa
        - update statistik sederhana
        - return jumlah baris yang dihapus

        args:
            data_deletion: object yang berisi tabel dan kondisi untuk penghapusan

        returns:
            jumlah baris yang terpengaruh (dihapus)
        """
        table_name = data_deletion.table

        # validasi tabel ada
        if table_name not in self.tables:
            available = list(self.tables.keys()) if self.tables else "tidak ada"
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan. Tersedia: {available}")

        # load rows
        all_rows = self._load_table_rows(table_name)
        if not all_rows:
            # either file empty atau tidak ada rows
            print(f"tidak ada baris ditemukan di tabel '{table_name}'")
            return 0

        # tentukan rows yang di-keep (delete yang match semua conditions)
        conditions = getattr(data_deletion, "conditions", []) or []
        rows_to_keep: List[Dict[str, Any]] = []
        deleted_count = 0

        for row in all_rows:
            if self._row_matches_all_conditions(row, conditions):
                deleted_count += 1
            else:
                rows_to_keep.append(row)

        if deleted_count == 0:
            print(f"tidak ada baris yang cocok untuk dihapus di tabel '{table_name}'")
            return 0

        # save remaining rows kembali ke disk
        self._save_table_rows(table_name, rows_to_keep)

        print(f"dihapus {deleted_count} baris dari tabel '{table_name}'")
        return deleted_count

    def set_index(self, table: str, column: str, index_type: str) -> None:
        """buat indeks untuk kolom dalam sebuah tabel.

        args:
            table: nama tabel
            column: nama kolom yang akan diindeks
            index_type: tipe indeks ("btree" atau "hash")

        todo: implementasi pembuatan indeks:
        - support minimal satu tipe indeks (b+ tree atau hash)
        - bangun struktur indeks untuk kolom yang ditentukan
        - simpan metadata indeks
        - (bonus: implementasi kedua tipe indeks)
        """
        raise NotImplementedError("set_index belum diimplementasi")

    def get_stats(self) -> Dict[str, Statistic]:
        """ambil statistik untuk semua tabel.

        returns:
            dictionary yang memetakan nama tabel ke object statistic mereka

        todo: implementasi pengumpulan statistik:
        - hitung n_r (jumlah tuple)
        - hitung b_r (jumlah blok): b_r = ⌈n_r / f_r⌉
        - hitung l_r (ukuran tuple dalam bytes)
        - hitung f_r (blocking factor)
        - hitung v(a,r) (nilai distinct per atribut)
        """
        raise NotImplementedError("get_stats belum diimplementasi")
