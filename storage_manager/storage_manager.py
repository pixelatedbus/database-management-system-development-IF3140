"""Core Storage Manager Implementation.
operasi read, write, delete, index, dan statistik.
"""
from __future__ import annotations

import os
import json
from typing import Any, Dict, List

from .models import Condition, DataRetrieval, DataWrite, DataDeletion, Statistic
from .utils import (
    evaluate_condition,
    project_columns,
    validate_table_name,
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

    def create_table(self, table_name: str, schema: List[str]) -> None:
        """Buat tabel baru dengan schema tertentu.

        Args:
            table_name: Nama tabel
            schema: List nama kolom

        Raises:
            ValueError: Jika nama tabel invalid atau sudah ada
        """
        if not validate_table_name(table_name):
            raise ValueError(f"Nama tabel tidak valid: {table_name}")

        if table_name in self.tables:
            raise ValueError(f"Tabel '{table_name}' sudah ada")

        # Simpan schema ke metadata
        self.tables[table_name] = {"schema": schema}
        self._save_table_schemas()

        # Buat file binary kosong
        table_file = self._get_table_file_path(table_name)
        write_binary_table(table_file, [], schema)

        print(f"✓ Tabel '{table_name}' berhasil dibuat dengan {len(schema)} kolom")

    def insert_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        """Insert rows ke tabel (untuk testing/demo purposes).

        Args:
            table_name: Nama tabel
            rows: List of row dictionaries

        Raises:
            ValueError: Jika tabel tidak ditemukan
        """
        if table_name not in self.tables:
            raise ValueError(f"Tabel '{table_name}' tidak ditemukan")

        schema = self.tables[table_name]["schema"]
        table_file = self._get_table_file_path(table_name)

        # Load existing data (if any)
        existing_rows = []
        if os.path.exists(table_file):
            try:
                existing_rows, _ = read_binary_table(table_file)
            except Exception:
                existing_rows = []

        # Append new rows
        all_rows = existing_rows + rows

        # Write back to binary file
        write_binary_table(table_file, all_rows, schema)
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
        
        Args:
            data_deletion: Object yang berisi tabel dan kondisi untuk penghapusan
            
        Returns:
            Jumlah baris yang terpengaruh (dihapus)
            
        TODO: Implementasi logika penghapusan:
        - Cari baris yang sesuai kondisi
        - Tandai atau hapus data dari disk
        - Update statistik
        - Koordinasi dengan Failure Recovery Manager
        """
        raise NotImplementedError("delete_block belum diimplementasi")

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
