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
    append_block_to_table,
    calculate_row_size

)


class StorageManager:
    # kelas utama buat ngatur penyimpanan data
    # tugasnya: simpan data ke binary file, baca/tulis/hapus blok, kelola index, kasih statistik
    # tiap tabel disimpan di file terpisah (data/nama_tabel.dat) dalam format binary

    def __init__(self, data_dir: str = "data", block_size: int = 4096):
        # inisialisasi storage manager
        # data_dir: folder tempat nyimpen file tabel
        # block_size: ukuran blok dalam bytes
        self.data_dir = data_dir
        self.block_size = block_size

        # tempat nyimpen info tabel, statistik, sama index
        self.tables: Dict[str, Dict[str, Any]] = {}
        self.stats: Dict[str, Statistic] = {}
        self.indexes: Dict[tuple, Any] = {}

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        self._load_table_schemas()
        print(f"storage manager diinisialisasi di: {os.path.abspath(self.data_dir)}")

    # ========== ngatur file ==========

    def _load_table_schemas(self) -> None:
        # load schema semua tabel dari metadata file
        metadata_file = self._get_metadata_file_path()
        if os.path.exists(metadata_file):
            try:
                self.tables = self._read_binary_metadata(metadata_file)
                print(f"✓ loaded {len(self.tables)} tabel dari metadata")
            except Exception as e:
                print(f"⚠ error loading metadata: {e}")
                self.tables = {}

    def _save_table_schemas(self) -> None:
        # simpen schema semua tabel ke metadata file
        metadata_file = self._get_metadata_file_path()
        try:
            self._write_binary_metadata(metadata_file, self.tables)
        except Exception as e:
            print(f"⚠ error saving metadata: {e}")

    def _get_metadata_file_path(self) -> str:
        # dapetin path file metadata
        return os.path.join(self.data_dir, "__metadata__.dat")

    def _get_table_file_path(self, table_name: str) -> str:
        # dapetin path file buat tabel tertentu
        return os.path.join(self.data_dir, f"{table_name}.dat")

    def _write_binary_metadata(self, file_path: str, tables: Dict[str, Dict[str, Any]]) -> None:
        # tulis metadata ke binary file
        # formatnya: magic bytes, version, jumlah tabel, terus info tiap tabel
        with open(file_path, 'wb') as f:
            # tulis magic bytes
            f.write(b'META')

            # tulis version
            f.write(struct.pack('<I', 1))

            # tulis jumlah tabel
            f.write(struct.pack('<I', len(tables)))

            # tulis tiap tabel
            for table_name, table_meta in tables.items():
                # tulis nama tabel
                table_name_bytes = table_name.encode('utf-8')
                f.write(struct.pack('<I', len(table_name_bytes)))
                f.write(table_name_bytes)

                # tulis columns
                columns = table_meta.get('columns', [])
                f.write(struct.pack('<I', len(columns)))
                for col in columns:
                    # tulis nama kolom
                    col_name = col['name'].encode('utf-8')
                    f.write(struct.pack('<I', len(col_name)))
                    f.write(col_name)

                    # tulis data_type
                    data_type = col['data_type'].encode('utf-8')
                    f.write(struct.pack('<I', len(data_type)))
                    f.write(data_type)

                    # tulis size (bisa null)
                    if col['size'] is None:
                        f.write(struct.pack('<B?', 0, False))
                    else:
                        f.write(struct.pack('<B?I', 1, True, col['size']))

                    # tulis is_primary_key
                    f.write(struct.pack('<?', col['is_primary_key']))

                    # tulis is_nullable
                    f.write(struct.pack('<?', col['is_nullable']))

                    # tulis default_value
                    default_val = col.get('default_value')
                    if default_val is None:
                        f.write(struct.pack('<B', 0))
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
                        f.write(struct.pack('<B', 0))

                # tulis primary_keys
                pk_list = table_meta.get('primary_keys', [])
                f.write(struct.pack('<I', len(pk_list)))
                for pk in pk_list:
                    pk_bytes = pk.encode('utf-8')
                    f.write(struct.pack('<I', len(pk_bytes)))
                    f.write(pk_bytes)

                # tulis foreign_keys
                fk_list = table_meta.get('foreign_keys', [])
                f.write(struct.pack('<I', len(fk_list)))
                for fk in fk_list:
                    # tulis column
                    col_bytes = fk['column'].encode('utf-8')
                    f.write(struct.pack('<I', len(col_bytes)))
                    f.write(col_bytes)

                    # tulis references_table
                    ref_table = fk['references_table'].encode('utf-8')
                    f.write(struct.pack('<I', len(ref_table)))
                    f.write(ref_table)

                    # tulis references_column
                    ref_col = fk['references_column'].encode('utf-8')
                    f.write(struct.pack('<I', len(ref_col)))
                    f.write(ref_col)

                    # tulis on_delete
                    on_del = fk['on_delete'].encode('utf-8')
                    f.write(struct.pack('<I', len(on_del)))
                    f.write(on_del)

                    # tulis on_update
                    on_upd = fk['on_update'].encode('utf-8')
                    f.write(struct.pack('<I', len(on_upd)))
                    f.write(on_upd)

    def _read_binary_metadata(self, file_path: str) -> Dict[str, Dict[str, Any]]:
        # baca metadata dari binary file
        with open(file_path, 'rb') as f:
            # cek magic bytes
            magic = f.read(4)
            if magic != b'META':
                raise ValueError(f"invalid metadata format: expected b'META', got {magic}")

            # baca version
            version = struct.unpack('<I', f.read(4))[0]
            if version != 1:
                raise ValueError(f"unsupported metadata version: {version}")

            # baca jumlah tabel
            num_tables = struct.unpack('<I', f.read(4))[0]

            tables = {}
            for _ in range(num_tables):
                # baca nama tabel
                table_name_len = struct.unpack('<I', f.read(4))[0]
                table_name = f.read(table_name_len).decode('utf-8')

                # baca columns
                num_cols = struct.unpack('<I', f.read(4))[0]
                columns = []
                for _ in range(num_cols):
                    col = {}

                    # baca nama kolom
                    col_name_len = struct.unpack('<I', f.read(4))[0]
                    col['name'] = f.read(col_name_len).decode('utf-8')

                    # baca data_type
                    data_type_len = struct.unpack('<I', f.read(4))[0]
                    col['data_type'] = f.read(data_type_len).decode('utf-8')

                    # baca size
                    _ = struct.unpack('<B', f.read(1))[0]
                    has_size = struct.unpack('<?', f.read(1))[0]
                    if has_size:
                        col['size'] = struct.unpack('<I', f.read(4))[0]
                    else:
                        col['size'] = None

                    # baca is_primary_key
                    col['is_primary_key'] = struct.unpack('<?', f.read(1))[0]

                    # baca is_nullable
                    col['is_nullable'] = struct.unpack('<?', f.read(1))[0]

                    # baca default_value
                    default_type = struct.unpack('<B', f.read(1))[0]
                    if default_type == 0:
                        col['default_value'] = None
                    elif default_type == 1:
                        col['default_value'] = struct.unpack('<q', f.read(8))[0]
                    elif default_type == 2:
                        col['default_value'] = struct.unpack('<d', f.read(8))[0]
                    elif default_type == 3:
                        str_len = struct.unpack('<I', f.read(4))[0]
                        col['default_value'] = f.read(str_len).decode('utf-8')
                    elif default_type == 4:
                        col['default_value'] = struct.unpack('<?', f.read(1))[0]
                    else:
                        col['default_value'] = None

                    columns.append(col)

                # baca primary_keys
                num_pks = struct.unpack('<I', f.read(4))[0]
                primary_keys = []
                for _ in range(num_pks):
                    pk_len = struct.unpack('<I', f.read(4))[0]
                    primary_keys.append(f.read(pk_len).decode('utf-8'))

                # baca foreign_keys
                num_fks = struct.unpack('<I', f.read(4))[0]
                foreign_keys = []
                for _ in range(num_fks):
                    fk = {}

                    # baca column
                    col_len = struct.unpack('<I', f.read(4))[0]
                    fk['column'] = f.read(col_len).decode('utf-8')

                    # baca references_table
                    ref_table_len = struct.unpack('<I', f.read(4))[0]
                    fk['references_table'] = f.read(ref_table_len).decode('utf-8')

                    # baca references_column
                    ref_col_len = struct.unpack('<I', f.read(4))[0]
                    fk['references_column'] = f.read(ref_col_len).decode('utf-8')

                    # baca on_delete
                    on_del_len = struct.unpack('<I', f.read(4))[0]
                    fk['on_delete'] = f.read(on_del_len).decode('utf-8')

                    # baca on_update
                    on_upd_len = struct.unpack('<I', f.read(4))[0]
                    fk['on_update'] = f.read(on_upd_len).decode('utf-8')

                    foreign_keys.append(fk)

                tables[table_name] = {
                    'columns': columns,
                    'primary_keys': primary_keys,
                    'foreign_keys': foreign_keys
                }

            return tables


    # ========== ngatur tabel ==========

    def create_table(
        self,
        table_name: str,
        columns: Union[List[str], List[ColumnDefinition]],
        primary_keys: Optional[List[str]] = None,
        foreign_keys: Optional[List[ForeignKey]] = None
    ) -> None:
        # bikin tabel baru dengan schema dan constraints
        # bisa pake list nama kolom aja atau list columndefinition yang lebih lengkap
        if not validate_table_name(table_name):
            raise ValueError(f"Nama tabel tidak valid: {table_name}")

        if table_name in self.tables:
            raise ValueError(f"Tabel '{table_name}' sudah ada")

        # kalo columns cuma list string, convert ke columndefinition dengan default varchar 255
        if columns and isinstance(columns[0], str):
            column_defs = [
                ColumnDefinition(name=col, data_type="VARCHAR", size=255, is_nullable=True)
                for col in columns
            ]
        else:
            column_defs = columns

        # validasi primary keys
        if primary_keys:
            for pk in primary_keys:
                pk_col = next((c for c in column_defs if c.name == pk), None)
                if pk_col is None:
                    raise ValueError(f"Primary key '{pk}' tidak ada di columns")
                pk_col.is_primary_key = True
                pk_col.is_nullable = False

        # validasi foreign keys
        if foreign_keys:
            for fk in foreign_keys:
                fk_col = next((c for c in column_defs if c.name == fk.column), None)
                if fk_col is None:
                    raise ValueError(f"Foreign key column '{fk.column}' tidak ada")

                if fk.references_table not in self.tables:
                    raise ValueError(f"Referenced table '{fk.references_table}' tidak ditemukan")

        # simpen metadata tabel
        self.tables[table_name] = {
            "columns": [self._column_def_to_dict(c) for c in column_defs],
            "primary_keys": primary_keys or [],
            "foreign_keys": [self._foreign_key_to_dict(fk) for fk in foreign_keys] if foreign_keys else []
        }
        self._save_table_schemas()

        # bikin file binary kosong
        schema_names = [c.name for c in column_defs]
        table_file = self._get_table_file_path(table_name)
        write_binary_table(table_file, [], schema_names, self.block_size)

        print(f"✓ tabel '{table_name}' berhasil dibuat dengan {len(column_defs)} kolom")

    def drop_table(self, table_name: str) -> None:
        if table_name not in self.tables:
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan")

        # implementasi restrict drop: ga boleh hapus kalo ada tabel lain yang referensi ke tabel ini
        for tbl, meta in self.tables.items():
            for fk in meta.get("foreign_keys", []):
                if fk["references_table"] == table_name:
                    raise ValueError(f"Tabel '{table_name}' tidak bisa dihapus karena direferensi oleh tabel '{tbl}'")

        # hapus metadata tabel
        del self.tables[table_name]
        self._save_table_schemas()

        # hapus file binary tabel
        table_file = self._get_table_file_path(table_name)
        if os.path.exists(table_file):
            os.remove(table_file)

        print(f"tabel '{table_name}' berhasil dihapus")

    def _column_def_to_dict(self, col: ColumnDefinition) -> Dict[str, Any]:
        # convert columndefinition ke dictionary buat disimpen
        return {
            "name": col.name,
            "data_type": col.data_type,
            "size": col.size,
            "is_primary_key": col.is_primary_key,
            "is_nullable": col.is_nullable,
            "default_value": col.default_value
        }

    def _dict_to_column_def(self, data: Dict[str, Any]) -> ColumnDefinition:
        # convert dictionary balik ke columndefinition
        return ColumnDefinition(
            name=data["name"],
            data_type=data["data_type"],
            size=data.get("size"),
            is_primary_key=data.get("is_primary_key", False),
            is_nullable=data.get("is_nullable", True),
            default_value=data.get("default_value")
        )

    def _foreign_key_to_dict(self, fk: ForeignKey) -> Dict[str, Any]:
        # convert foreignkey ke dictionary buat disimpen
        return {
            "column": fk.column,
            "references_table": fk.references_table,
            "references_column": fk.references_column,
            "on_delete": fk.on_delete,
            "on_update": fk.on_update
        }

    def _get_column_definitions(self, table_name: str) -> List[ColumnDefinition]:
        # ambil column definitions buat tabel tertentu
        if table_name not in self.tables:
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan")

        table_meta = self.tables[table_name]
        return [self._dict_to_column_def(c) for c in table_meta["columns"]]

    def insert_rows(self, table_name: str, rows: List[Dict[str, Any]], validate: bool = True) -> None:
        # insert banyak rows sekaligus ke tabel (optimized batch insert)
        if table_name not in self.tables:
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan")

        # ambil column definitions
        column_defs = self._get_column_definitions(table_name)

        # aplikasiin default values buat kolom yang ga diisi
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

        # validasi tiap row kalo diminta
        if validate:
            for i, row in enumerate(processed_rows):
                try:
                    validate_row_for_schema(row, column_defs)
                except ValueError as e:
                    raise ValueError(f"Row {i+1} validation failed: {e}")

        # ambil schema names buat binary file
        table_meta = self.tables[table_name]
        schema_names = [c["name"] for c in table_meta["columns"]]

        table_file = self._get_table_file_path(table_name)

        # pake append_block_to_table buat batch insert tanpa load semua data
        if not os.path.exists(table_file):
            write_binary_table(table_file, processed_rows, schema_names, self.block_size)
        else:
            append_block_to_table(table_file, processed_rows, schema_names, self.block_size)

        print(f"✓ inserted {len(rows)} rows ke tabel '{table_name}' (optimized batch insert)")

    # ========== operasi utama ==========

    def read_block(self, data_retrieval: DataRetrieval) -> List[Dict[str, Any]]:
        # baca data dari disk pake streaming (ga load semua ke memory)
        # filter row berdasarkan kondisi terus proyeksi kolom kalo diminta
        table_name = data_retrieval.table

        # cek tabel ada ga
        if table_name not in self.tables:
            available = list(self.tables.keys()) if self.tables else "tidak ada"
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan. Tersedia: {available}")

        table_file = self._get_table_file_path(table_name)

        # cek file exists
        if not os.path.exists(table_file):
            print(f"⚠ file tabel '{table_name}' tidak ditemukan")
            return []

        # stream data dari binary file
        try:
            def row_filter(row):
                return self._row_matches_all_conditions(row, data_retrieval.conditions)

            filtered_rows = list(read_binary_table_streaming(table_file, filter_fn=row_filter))
            print(f"✓ found {len(filtered_rows)} matching rows dari tabel '{table_name}' (streamed)")
        except Exception as e:
            raise ValueError(f"error membaca binary file '{table_name}.dat': {e}")

        # proyeksi kolom kalo ada
        if data_retrieval.column and len(data_retrieval.column) > 0:
            return [project_columns(row, data_retrieval.column) for row in filtered_rows]
        else:
            return filtered_rows

    def _row_matches_all_conditions(self, row: Dict[str, Any], conditions: List[Condition]) -> bool:
        # cek apakah row memenuhi semua kondisi (and logic)
        for condition in conditions:
            if not evaluate_condition(row, condition):
                return False
        return True


    # ========== helper buat delete_block ==========

    def _load_table_rows(self, table_name: str) -> List[Dict[str, Any]]:
        # load semua baris dari file tabel pake streaming
        table_file = self._get_table_file_path(table_name)
        if not os.path.exists(table_file):
            return []

        rows = list(read_binary_table_streaming(table_file))
        return rows

    def _save_table_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        # tulis balik semua baris ke file tabel
        schema_names = [c["name"] for c in self.tables[table_name]["columns"]]
        table_file = self._get_table_file_path(table_name)
        write_binary_table(table_file, rows, schema_names, self.block_size)

    # ========== helper buat write_block ==========

    def _load_all_rows_with_schema(self, table_name: str) -> Tuple[List[Dict[str, Any]], List[str]]:
        # muat semua baris data dan schema names dari file biner
        table_file = self._get_table_file_path(table_name)

        schema_names = [col.name for col in self._get_column_definitions(table_name)]

        if not os.path.exists(table_file):
            return [], schema_names

        rows = list(read_binary_table_streaming(table_file))
        return rows, schema_names


    def _apply_defaults_and_validate(self, rows: List[Dict[str, Any]], column_defs: List[ColumnDefinition]) -> None:
        # isi nilai default dan validasi schema buat tiap row (in-place)
        schema_names = [col.name for col in column_defs]

        for row in rows:
            # aplikasiin nilai default buat kolom yang ga ada
            for col_def in column_defs:
                col_name = col_def.name

                if col_name not in row:
                    if col_def.default_value is not None:
                        row[col_name] = col_def.default_value
                    elif col_def.is_nullable:
                        row[col_name] = None
                    else:
                        raise ValueError(f"Kolom wajib '{col_name}' tidak disediakan dan tidak memiliki nilai default.")

            # validasi penuh pake fungsi utilitas
            validate_row_for_schema(row, column_defs)


    # ========== write / delete / index / stats ==========
    def write_block(self, data_write: DataWrite) -> int:
        # tulis atau update data di disk
        # kalo ga ada kondisi: insert row baru
        # kalo ada kondisi: update row yang match

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

            # cek ini batch insert atau single insert
            is_batch = isinstance(data_write.new_value[0], (list, tuple)) if data_write.new_value else False

            if is_batch:
                # batch insert - masukin banyak rows sekaligus
                rows_to_insert = []
                for row_values in data_write.new_value:
                    if len(data_write.column) != len(row_values):
                        raise ValueError(f"Jumlah kolom ({len(data_write.column)}) dan nilai ({len(row_values)}) harus sama")

                    row_data = dict(zip(data_write.column, row_values))
                    rows_to_insert.append(row_data)

                # validasi semua rows
                self._apply_defaults_and_validate(rows_to_insert, column_defs)

                # batch insert pake append_block_to_table
                if not os.path.exists(table_file):
                    write_binary_table(table_file, rows_to_insert, schema_names, self.block_size)
                else:
                    append_block_to_table(table_file, rows_to_insert, schema_names, self.block_size)

                print(f"✓ BATCH inserted {len(rows_to_insert)} rows ke tabel '{table_name}' (efficient!)")
                return len(rows_to_insert)

            else:
                # single insert - masukin satu row aja
                if len(data_write.column) != len(data_write.new_value):
                    raise ValueError("Jumlah kolom dan nilai baru harus sama")

                new_row_data = dict(zip(data_write.column, data_write.new_value))

                # aplikasiin nilai default dan validasi
                self._apply_defaults_and_validate([new_row_data], column_defs)

                # file baru atau udah ada?
                if not os.path.exists(table_file):
                    write_binary_table(table_file, [new_row_data], schema_names, self.block_size)
                else:
                    append_row_to_table(table_file, new_row_data, schema_names, self.block_size)

                print(f"inserted 1 row ke tabel '{table_name}'")
                return 1

        # ========== logika update ==========
        else:
            # validasi input
            if not data_write.column or not data_write.new_value:
                raise ValueError("untuk update, column dan new_value harus diisi")

            if len(data_write.column) != len(data_write.new_value):
                raise ValueError("jumlah kolom dan nilai baru harus sama")

            update_data = dict(zip(data_write.column, data_write.new_value))

            # validasi tipe data buat nilai yang di-update
            for col_name, new_val in update_data.items():
                col_def = next((c for c in column_defs if c.name == col_name), None)
                if col_def is None:
                    raise ValueError(f"kolom '{col_name}' tidak ada di tabel '{table_name}'")

                try:
                    validate_value_for_column(new_val, col_def)
                except ValueError as e:
                    raise ValueError(f"update value validation failed for column '{col_name}': {e}")

            # load semua rows buat scan kondisi
            all_rows, _ = self._load_all_rows_with_schema(table_name)

            # scan dan update rows yang match
            new_rows = []
            rows_affected = 0

            for row in all_rows:
                if self._row_matches_all_conditions(row, data_write.conditions):
                    updated_row = row.copy()
                    for col_name, new_val in update_data.items():
                        updated_row[col_name] = new_val
                    new_rows.append(updated_row)
                    rows_affected += 1
                else:
                    new_rows.append(row)

            # tulis balik semua rows
            if rows_affected > 0:
                write_binary_table(table_file, new_rows, schema_names, self.block_size)

            print(f"updated {rows_affected} rows di tabel '{table_name}'")
            return rows_affected



    def delete_block(self, data_deletion: DataDeletion) -> int:
        # hapus data dari disk
        # load semua baris, filter yang match kondisi, simpen yang tersisa
        table_name = data_deletion.table

        # cek tabel ada ga
        if table_name not in self.tables:
            available = list(self.tables.keys()) if self.tables else "tidak ada"
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan. Tersedia: {available}")

        # load rows
        all_rows = self._load_table_rows(table_name)
        if not all_rows:
            print(f"tidak ada baris ditemukan di tabel '{table_name}'")
            return 0

        # tentuin rows yang di-keep (hapus yang match semua conditions)
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

        # simpen rows yang tersisa balik ke disk
        self._save_table_rows(table_name, rows_to_keep)

        print(f"dihapus {deleted_count} baris dari tabel '{table_name}'")
        return deleted_count

    def set_index(self, table: str, column: str, index_type: str) -> None:
        # bikin index buat kolom di tabel
        # bisa btree atau hash
        raise NotImplementedError("set_index belum diimplementasi")

    def get_stats(self) -> Dict[str, Statistic]:
        # ambil statistik buat semua tabel
        # n_r: jumlah tuple, b_r: jumlah blok, l_r: ukuran rata-rata tuple
        # f_r: blocking factor, V_a_r: jumlah nilai distinct per atribut

        stats = {}

        for table_name in self.tables:
            table_file = self._get_table_file_path(table_name)
            schema_names = [c["name"] for c in self.tables[table_name]["columns"]]

            # default values kalo tabel kosong
            n_r = 0
            b_r = 0
            l_r = 0
            f_r = 0
            V_a_r: Dict[str, int] = {}

            if not os.path.exists(table_file):
                stats[table_name] = Statistic(
                    n_r=n_r,
                    b_r=b_r,
                    l_r=l_r,
                    f_r=f_r,
                    V_a_r=V_a_r
                )
                continue

            try:
                # baca header file buat dapetin num_blocks
                with open(table_file, 'rb') as f:
                    magic = f.read(4)
                    if magic != b'SMDB':
                        continue

                    f.read(4)

                    schema_length = struct.unpack('<I', f.read(4))[0]
                    f.seek(schema_length, 1)

                    f.read(4)

                    # ini b_r (jumlah blok)
                    b_r = struct.unpack('<I', f.read(4))[0]

                # load semua rows buat hitung statistik lainnya
                all_rows = list(read_binary_table_streaming(table_file))
                n_r = len(all_rows)

                if n_r > 0:
                    # hitung l_r (rata-rata ukuran row dalam bytes)
                    total_size = sum(calculate_row_size(row, schema_names) for row in all_rows)
                    l_r = int(total_size / n_r)

                    # hitung f_r (blocking factor)
                    if l_r > 0:
                        usable_space = self.block_size - 4
                        f_r = int(usable_space / l_r)
                        if f_r == 0:
                            f_r = 1

                    # hitung V(a,r) - jumlah nilai distinct per atribut
                    for col_name in schema_names:
                        distinct_values = set()
                        for row in all_rows:
                            if col_name in row:
                                val = row[col_name]
                                if isinstance(val, dict) or isinstance(val, list):
                                    val = str(val)
                                distinct_values.add(val)
                        V_a_r[col_name] = len(distinct_values)

                stats[table_name] = Statistic(
                    n_r=n_r,
                    b_r=b_r,
                    l_r=l_r,
                    f_r=f_r,
                    V_a_r=V_a_r
                )

            except Exception as e:
                print(f"⚠ error calculating stats for '{table_name}': {e}")
                stats[table_name] = Statistic(
                    n_r=0,
                    b_r=0,
                    l_r=0,
                    f_r=0,
                    V_a_r={}
                )

        self.stats = stats
        return stats
