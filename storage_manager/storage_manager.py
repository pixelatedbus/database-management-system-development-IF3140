"""Core Storage Manager Implementation.
operasi read, write, delete, index, dan statistik.
"""
from __future__ import annotations

import os
import json
from typing import Any, Dict, List, Optional, Union

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
    read_binary_table,
    write_binary_table
)


class StorageManager:
    """Komponen Storage Manager untuk menangani penyimpanan data.
    
    Tanggung jawab:
    - Menyimpan data dalam binary file(s) di harddisk
    - Membaca, menulis, dan menghapus blok data
    - Mengelola indeks (B+ Tree atau Hash)
    - Menyediakan statistik untuk optimisasi query
    
    Implementasi:
    - 1 file per tabel (format: data/table_name.dat)
    - Data disimpan dalam format JSON untuk kemudahan testing
    - Setiap tabel memiliki metadata schema yang disimpan terpisah
    """

    def __init__(self, data_dir: str = "data", block_size: int = 4096):
        """Inisialisasi Storage Manager.
        
        Args:
            data_dir: Direktori tempat menyimpan file data tabel (default: 'data')
            block_size: Ukuran blok dalam bytes (default: 4096 bytes)
        """
        self.data_dir = data_dir
        self.block_size = block_size
        
        # Struktur manajemen file
        self.tables: Dict[str, Dict[str, Any]] = {}  # nama_tabel -> schema
        self.stats: Dict[str, Statistic] = {}  # nama_tabel -> Statistic
        self.indexes: Dict[tuple, Any] = {}  # (table_name, column_name) -> index structure
        
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        
        self._load_table_schemas()
        print(f"Storage Manager diinisialisasi di: {os.path.abspath(self.data_dir)}")

    # ========== File Management ==========
    
    def _load_table_schemas(self) -> None:
        """Load schema dari semua tabel yang ada di data_dir."""
        metadata_file = self._get_metadata_file_path()
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r') as f:
                    self.tables = json.load(f)
                print(f"✓ Loaded {len(self.tables)} tabel dari metadata")
            except Exception as e:
                print(f"⚠ Error loading metadata: {e}")
                self.tables = {}

    def _save_table_schemas(self) -> None:
        """Simpan schema dari semua tabel ke metadata file."""
        metadata_file = self._get_metadata_file_path()
        try:
            with open(metadata_file, 'w') as f:
                json.dump(self.tables, f, indent=2)
        except Exception as e:
            print(f"⚠ Error saving metadata: {e}")

    def _get_metadata_file_path(self) -> str:
        """Dapatkan path file metadata."""
        return os.path.join(self.data_dir, "__metadata__.json")

    def _get_table_file_path(self, table_name: str) -> str:
        """Dapatkan path file untuk tabel tertentu."""
        return os.path.join(self.data_dir, f"{table_name}.dat")

    # ========== Table Management ==========

    def create_table(
        self,
        table_name: str,
        columns: Union[List[str], List[ColumnDefinition]],
        primary_keys: Optional[List[str]] = None,
        foreign_keys: Optional[List[ForeignKey]] = None
    ) -> None:
        """Buat tabel baru dengan schema dan constraints.

        Args:
            table_name: Nama tabel
            columns: List nama kolom (backward compatible) ATAU List ColumnDefinition
            primary_keys: List nama kolom yang jadi PRIMARY KEY (opsional)
            foreign_keys: List ForeignKey constraints (opsional)

        Raises:
            ValueError: Jika nama tabel invalid atau sudah ada

        Example:
            # Cara 1: Backward compatible (hanya nama kolom)
            sm.create_table("users", ["id", "name", "email"])

            # Cara 2: Dengan tipe data lengkap
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

        # Handle backward compatibility: jika columns adalah List[str]
        if columns and isinstance(columns[0], str):
            # Convert ke ColumnDefinition dengan tipe default (VARCHAR 255)
            column_defs = [
                ColumnDefinition(name=col, data_type="VARCHAR", size=255, is_nullable=True)
                for col in columns
            ]
        else:
            column_defs = columns

        # Validasi primary keys
        if primary_keys:
            for pk in primary_keys:
                # Cari column definition untuk PK
                pk_col = next((c for c in column_defs if c.name == pk), None)
                if pk_col is None:
                    raise ValueError(f"Primary key '{pk}' tidak ada di columns")
                # Set sebagai primary key
                pk_col.is_primary_key = True
                pk_col.is_nullable = False

        # Validasi foreign keys
        if foreign_keys:
            for fk in foreign_keys:
                # Check column exists
                fk_col = next((c for c in column_defs if c.name == fk.column), None)
                if fk_col is None:
                    raise ValueError(f"Foreign key column '{fk.column}' tidak ada")

                # Check referenced table exists
                if fk.references_table not in self.tables:
                    raise ValueError(f"Referenced table '{fk.references_table}' tidak ditemukan")

        # Simpan metadata dengan format lengkap
        self.tables[table_name] = {
            "columns": [self._column_def_to_dict(c) for c in column_defs],
            "primary_keys": primary_keys or [],
            "foreign_keys": [self._foreign_key_to_dict(fk) for fk in foreign_keys] if foreign_keys else []
        }
        self._save_table_schemas()

        # Buat file binary kosong (gunakan nama kolom saja untuk compatibility)
        schema_names = [c.name for c in column_defs]
        table_file = self._get_table_file_path(table_name)
        write_binary_table(table_file, [], schema_names)

        print(f"✓ Tabel '{table_name}' berhasil dibuat dengan {len(column_defs)} kolom")

    def _column_def_to_dict(self, col: ColumnDefinition) -> Dict[str, Any]:
        """Convert ColumnDefinition ke dictionary untuk JSON storage."""
        return {
            "name": col.name,
            "data_type": col.data_type,
            "size": col.size,
            "is_primary_key": col.is_primary_key,
            "is_nullable": col.is_nullable,
            "default_value": col.default_value
        }

    def _dict_to_column_def(self, data: Dict[str, Any]) -> ColumnDefinition:
        """Convert dictionary ke ColumnDefinition."""
        return ColumnDefinition(
            name=data["name"],
            data_type=data["data_type"],
            size=data.get("size"),
            is_primary_key=data.get("is_primary_key", False),
            is_nullable=data.get("is_nullable", True),
            default_value=data.get("default_value")
        )

    def _foreign_key_to_dict(self, fk: ForeignKey) -> Dict[str, Any]:
        """Convert ForeignKey ke dictionary untuk JSON storage."""
        return {
            "column": fk.column,
            "references_table": fk.references_table,
            "references_column": fk.references_column,
            "on_delete": fk.on_delete,
            "on_update": fk.on_update
        }

    def _get_column_definitions(self, table_name: str) -> List[ColumnDefinition]:
        """Get column definitions untuk tabel tertentu."""
        if table_name not in self.tables:
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan")

        table_meta = self.tables[table_name]

        # Handle backward compatibility
        if "schema" in table_meta:
            # Old format: hanya list nama kolom
            return [
                ColumnDefinition(name=col, data_type="VARCHAR", size=255)
                for col in table_meta["schema"]
            ]
        else:
            # New format: dengan column definitions
            return [self._dict_to_column_def(c) for c in table_meta["columns"]]

    def insert_rows(self, table_name: str, rows: List[Dict[str, Any]], validate: bool = True) -> None:
        """Insert rows ke tabel dengan validasi tipe data.

        Args:
            table_name: Nama tabel
            rows: List of row dictionaries
            validate: Apakah melakukan validasi tipe data (default: True)

        Raises:
            ValueError: Jika tabel tidak ditemukan atau data tidak valid
        """
        if table_name not in self.tables:
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan")

        # Get column definitions
        column_defs = self._get_column_definitions(table_name)

        # Validasi setiap row jika diminta
        if validate:
            for i, row in enumerate(rows):
                try:
                    validate_row_for_schema(row, column_defs)
                except ValueError as e:
                    raise ValueError(f"Row {i+1} validation failed: {e}")

        # Get schema names untuk binary file
        table_meta = self.tables[table_name]
        if "schema" in table_meta:
            # Old format
            schema_names = table_meta["schema"]
        else:
            # New format
            schema_names = [c["name"] for c in table_meta["columns"]]

        table_file = self._get_table_file_path(table_name)

        # Load existing data (if any)
        existing_rows = []
        if os.path.exists(table_file):
            try:
                existing_rows, _ = read_binary_table(table_file)
            except Exception:
                existing_rows = []

        # Apply default values untuk kolom yang tidak diisi
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

        # Append new rows
        all_rows = existing_rows + processed_rows

        # Write back to binary file
        write_binary_table(table_file, all_rows, schema_names)
        print(f"✓ Inserted {len(rows)} rows ke tabel '{table_name}'")

    # ========== Core Operations ==========

    def read_block(self, data_retrieval: DataRetrieval) -> List[Dict[str, Any]]:
        """Membaca data dari disk berdasarkan parameter retrieval.

        Proses:
        1. Validasi tabel ada di storage
        2. Load semua data dari binary file tabel
        3. Filter row berdasarkan kondisi (AND logic)
        4. Proyeksi kolom jika diminta
        5. Return hasil

        Args:
            data_retrieval: Object yang berisi nama tabel, kolom, dan kondisi

        Returns:
            List dari baris (setiap baris sebagai dictionary) yang sesuai kondisi

        Raises:
            ValueError: Jika tabel tidak ditemukan
        """
        table_name = data_retrieval.table

        # 1. Validasi tabel ada
        if table_name not in self.tables:
            available = list(self.tables.keys()) if self.tables else "tidak ada"
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan. Tersedia: {available}")

        table_file = self._get_table_file_path(table_name)

        # 2. Cek file exists
        if not os.path.exists(table_file):
            print(f"⚠ File tabel '{table_name}' tidak ditemukan")
            return []

        # 3. Load data dari BINARY FILE
        try:
            all_rows, _ = read_binary_table(table_file)
            print(f"✓ Loaded {len(all_rows)} rows dari binary file '{table_name}.dat'")
        except ValueError as e:
            # Jika format tidak valid, coba fallback ke JSON (backward compatibility)
            print(f"⚠ Binary format error: {e}. Trying JSON fallback...")
            try:
                with open(table_file, 'r') as f:
                    all_rows = json.load(f)
                if not isinstance(all_rows, list):
                    all_rows = []
                print(f"✓ Loaded {len(all_rows)} rows dari JSON file (fallback)")
            except Exception as json_error:
                print(f"Error membaca file tabel: {json_error}")
                return []
        except Exception as e:
            print(f"Error membaca binary file: {e}")
            return []

        # 4. Filter berdasarkan kondisi (AND logic)
        filtered_rows = []
        for row in all_rows:
            if self._row_matches_all_conditions(row, data_retrieval.conditions):
                filtered_rows.append(row)

        # 5. Proyeksi kolom jika ada
        if data_retrieval.column and len(data_retrieval.column) > 0:
            return [project_columns(row, data_retrieval.column) for row in filtered_rows]
        else:
            return filtered_rows

    def _row_matches_all_conditions(self, row: Dict[str, Any], conditions: List[Condition]) -> bool:
        """Cek apakah row memenuhi SEMUA kondisi (AND logic).
        
        Args:
            row: Baris data (dictionary)
            conditions: List kondisi yang harus dipenuhi
            
        Returns:
            True jika row memenuhi semua kondisi, False sebaliknya
        """
        for condition in conditions:
            if not evaluate_condition(row, condition):
                return False
        return True

    # ========== Helpers for delete_block ==========
    def _load_table_rows(self, table_name: str) -> List[Dict[str, Any]]:
        """Load semua baris dari file tabel. Menangani fallback JSON.

        Args:
            table_name: Nama tabel
            
        Returns:
            List dari baris (setiap baris sebagai dictionary)
        """
        table_file = self._get_table_file_path(table_name)
        if not os.path.exists(table_file):
            return []

        try:
            rows, _ = read_binary_table(table_file)
            return rows
        except ValueError as e:
            # fallback ke JSON
            try:
                with open(table_file, 'r') as f:
                    rows = json.load(f)
                if not isinstance(rows, list):
                    return []
                return rows
            except Exception:
                return []
        except Exception:
            return []

    def _save_table_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        """Tulis kembali semua baris ke file tabel menggunakan schema yang tersimpan.
        
        Args:
            table_name: Nama tabel
            rows: List dari baris (setiap baris sebagai dictionary)
        
        Raises:
            Exception: Jika penulisan gagal
        """
        schema = self.tables.get(table_name, {}).get("schema", [])
        table_file = self._get_table_file_path(table_name)
        try:
            write_binary_table(table_file, rows, schema)
        except Exception as e:
            # fallback: try writing JSON to avoid data loss during development
            try:
                with open(table_file, 'w') as f:
                    json.dump(rows, f, indent=2)
                print(f"⚠ write_binary_table gagal ({e}), ditulis sebagai JSON fallback")
            except Exception as ex:
                print(f"Error menyimpan tabel '{table_name}': {ex}")

    def _update_stats_after_delete(self, table_name: str, remaining_rows: List[Dict[str, Any]]) -> None:
        """Update statistik sederhana setelah penghapusan (hanya count saat ini).
        
        Args:
            table_name: Nama tabel
            remaining_rows: List dari baris yang tersisa setelah penghapusan
        """
        try:
            n_r = len(remaining_rows)
            stat_obj = self.stats.get(table_name)
            # Jika stat_obj sudah ada dan memiliki atribut n_r, coba perbarui; jika tidak, simpan minimal info
            if isinstance(stat_obj, dict):
                stat_obj["n_r"] = n_r
            else:
                # Create minimal statistic representation
                self.stats[table_name] = {"n_r": n_r}
        except Exception:
            # Jangan biarkan error statistik menghentikan operasi penghapusan
            pass

    # ========== Write / Delete / Index / Stats ==========
    def write_block(self, data_write: DataWrite) -> int:
        """Menulis atau memodifikasi data di disk.
        
        Args:
            data_write: Object yang berisi tabel, kolom, kondisi, dan nilai baru
            
        Returns:
            Jumlah baris yang terpengaruh
            
        TODO: Implementasi logika insert/update:
        - Tentukan apakah operasi INSERT atau UPDATE
        - Cari lokasi yang sesuai di disk
        - Tulis data dalam format binary
        - Update statistik
        - Koordinasi dengan Failure Recovery Manager untuk dirty data
        """
        raise NotImplementedError("write_block belum diimplementasi")

    def delete_block(self, data_deletion: DataDeletion) -> int:
        """Menghapus data dari disk.

        Proses:
        - Validasi tabel ada
        - Load semua baris
        - Tentukan baris yang harus dihapus berdasarkan kondisi (AND)
        - Simpan ulang baris yang tersisa
        - Update statistik sederhana
        - Return jumlah baris yang dihapus

        Args:
            data_deletion: Object yang berisi tabel dan kondisi untuk penghapusan

        Returns:
            Jumlah baris yang terpengaruh (dihapus)
        """
        table_name = data_deletion.table

        # Validasi tabel ada
        if table_name not in self.tables:
            available = list(self.tables.keys()) if self.tables else "tidak ada"
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan. Tersedia: {available}")

        # Load rows
        all_rows = self._load_table_rows(table_name)
        if not all_rows:
            # Either file empty or tidak ada rows
            print(f"✓ Tidak ada baris ditemukan di tabel '{table_name}'")
            return 0

        # Determine rows to keep (delete those that match ALL conditions)
        conditions = getattr(data_deletion, "conditions", []) or []
        rows_to_keep: List[Dict[str, Any]] = []
        deleted_count = 0

        for row in all_rows:
            if self._row_matches_all_conditions(row, conditions):
                deleted_count += 1
            else:
                rows_to_keep.append(row)

        if deleted_count == 0:
            print(f"✓ Tidak ada baris yang cocok untuk dihapus di tabel '{table_name}'")
            return 0

        # Save remaining rows back to disk
        self._save_table_rows(table_name, rows_to_keep)

        # Update simple statistics
        self._update_stats_after_delete(table_name, rows_to_keep)

        print(f"✓ Dihapus {deleted_count} baris dari tabel '{table_name}'")
        return deleted_count

    def set_index(self, table: str, column: str, index_type: str) -> None:
        """Membuat indeks untuk kolom dalam sebuah tabel.
        
        Args:
            table: Nama tabel
            column: Nama kolom yang akan diindeks
            index_type: Tipe indeks ("btree" atau "hash")
            
        TODO: Implementasi pembuatan indeks:
        - Support minimal satu tipe indeks (B+ Tree atau Hash)
        - Bangun struktur indeks untuk kolom yang ditentukan
        - Simpan metadata indeks
        - (Bonus: implementasi kedua tipe indeks)
        """
        raise NotImplementedError("set_index belum diimplementasi")

    def get_stats(self) -> Dict[str, Statistic]:
        """Mengambil statistik untuk semua tabel.
        
        Returns:
            Dictionary yang memetakan nama tabel ke object Statistic mereka
            
        TODO: Implementasi pengumpulan statistik:
        - Hitung n_r (jumlah tuple)
        - Hitung b_r (jumlah blok): b_r = ⌈n_r / f_r⌉
        - Hitung l_r (ukuran tuple dalam bytes)
        - Hitung f_r (blocking factor)
        - Hitung V(A,r) (nilai distinct per atribut)
        """
        raise NotImplementedError("get_stats belum diimplementasi")
