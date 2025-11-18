"""
Comprehensive Test Suite untuk Storage Manager

Test suite ini mencakup semua fungsi utama Storage Manager:
1. create_table() - Membuat tabel baru
2. insert_rows() - Insert data ke tabel
3. read_block() - Membaca data dengan filtering & projection
4. write_block() - Update/Insert data (TODO)
5. delete_block() - Hapus data
6. set_index() - Buat index (TODO)
7. get_stats() - Ambil statistik (TODO)

Cara menjalankan:
    python3 -m storage_manager.test_storage_manager

Atau jalankan test spesifik:
    python3 -m storage_manager.test_storage_manager --test read_block
"""

import os
import sys
from typing import List, Dict, Any

from .storage_manager import StorageManager
from .models import Condition, DataRetrieval, DataWrite, DataDeletion, ColumnDefinition


class TestStorageManager:
    """Test suite untuk Storage Manager."""

    def __init__(self, test_dir: str = "test_data"):
        """Initialize test suite.

        Args:
            test_dir: Directory untuk menyimpan test data
        """
        self.test_dir = test_dir
        self.sm = None
        self.passed = 0
        self.failed = 0

    def setup(self):
        """Setup test environment."""
        # Hapus test directory lama jika ada
        if os.path.exists(self.test_dir):
            import shutil
            shutil.rmtree(self.test_dir)

        # Buat Storage Manager baru
        self.sm = StorageManager(data_dir=self.test_dir)
        print(f"\n{'='*70}")
        print(f"SETUP: Test environment initialized at {self.test_dir}")
        print(f"{'='*70}\n")

    def teardown(self):
        """Cleanup after tests."""
        print(f"\n{'='*70}")
        print(f"TEARDOWN: Cleaning up test data...")
        print(f"{'='*70}\n")

    def assert_equal(self, actual, expected, message: str = ""):
        """Assert that actual equals expected."""
        if actual == expected:
            self.passed += 1
            print(f"  ✓ PASS: {message}")
            return True
        else:
            self.failed += 1
            print(f"  ✗ FAIL: {message}")
            print(f"    Expected: {expected}")
            print(f"    Got: {actual}")
            return False

    def assert_true(self, condition: bool, message: str = ""):
        """Assert that condition is True."""
        if condition:
            self.passed += 1
            print(f"  ✓ PASS: {message}")
            return True
        else:
            self.failed += 1
            print(f"  ✗ FAIL: {message}")
            return False

    def print_header(self, test_name: str):
        """Print test header."""
        print(f"\n{'='*70}")
        print(f"TEST: {test_name}")
        print(f"{'='*70}")

    def print_summary(self):
        """Print test summary."""
        total = self.passed + self.failed
        print(f"\n{'='*70}")
        print(f"TEST SUMMARY")
        print(f"{'='*70}")
        print(f"Total tests: {total}")
        print(f"Passed: {self.passed} ({self.passed/total*100:.1f}%)" if total > 0 else "Passed: 0")
        print(f"Failed: {self.failed} ({self.failed/total*100:.1f}%)" if total > 0 else "Failed: 0")
        print(f"{'='*70}\n")

        if self.failed == 0:
            print("🎉 ALL TESTS PASSED!")
        else:
            print(f"⚠️  {self.failed} TEST(S) FAILED")

    # ========== Test: create_table ==========

    def test_create_table(self):
        """Test create_table functionality."""
        self.print_header("CREATE TABLE")

        # Test 1: Create simple table
        print("\n[1] Create simple table 'users'")
        try:
            self.sm.create_table("users", ["id", "name", "email"])
            self.assert_true("users" in self.sm.tables, "Table 'users' should exist")
        except Exception as e:
            self.assert_true(False, f"Should create table successfully: {e}")

        # Test 2: Create table with more columns
        print("\n[2] Create table 'mahasiswa' with multiple columns")
        try:
            self.sm.create_table("mahasiswa", ["nim", "nama", "ipk", "angkatan", "jurusan"])
            self.assert_true("mahasiswa" in self.sm.tables, "Table 'mahasiswa' should exist")
        except Exception as e:
            self.assert_true(False, f"Should create table successfully: {e}")

        # Test 3: Duplicate table should fail
        print("\n[3] Creating duplicate table should fail")
        try:
            self.sm.create_table("users", ["id", "name"])
            self.assert_true(False, "Should raise ValueError for duplicate table")
        except ValueError:
            self.assert_true(True, "Should raise ValueError for duplicate table")

        # Test 4: Invalid table name should fail
        print("\n[4] Invalid table name should fail")
        try:
            self.sm.create_table("123invalid", ["id"])
            self.assert_true(False, "Should raise ValueError for invalid table name")
        except ValueError:
            self.assert_true(True, "Should raise ValueError for invalid table name")

    # ========== Test: insert_rows & read_block ==========

    def test_insert_and_read(self):
        """Test insert_rows and read_block functionality."""
        self.print_header("INSERT ROWS & READ BLOCK")

        # Setup: Create table (skip if already exists from previous test)
        if "mahasiswa" not in self.sm.tables:
            self.sm.create_table("mahasiswa", ["nim", "nama", "ipk", "angkatan", "jurusan"])

        # Test data
        test_data = [
            {"nim": "13520001", "nama": "Budi Santoso", "ipk": 3.75, "angkatan": 2020, "jurusan": "IF"},
            {"nim": "13520002", "nama": "Ani Wijaya", "ipk": 3.90, "angkatan": 2020, "jurusan": "IF"},
            {"nim": "13520003", "nama": "Citra Dewi", "ipk": 3.25, "angkatan": 2020, "jurusan": "STI"},
            {"nim": "13521001", "nama": "Doni Pratama", "ipk": 3.50, "angkatan": 2021, "jurusan": "IF"},
            {"nim": "13521002", "nama": "Eka Putri", "ipk": 3.85, "angkatan": 2021, "jurusan": "IF"},
            {"nim": "13521003", "nama": "Fajar Ramadhan", "ipk": 3.10, "angkatan": 2021, "jurusan": "STI"},
            {"nim": "13522001", "nama": "Gina Maulida", "ipk": 3.95, "angkatan": 2022, "jurusan": "IF"},
            {"nim": "13522002", "nama": "Hadi Kusuma", "ipk": 3.40, "angkatan": 2022, "jurusan": "STI"},
        ]

        # Test 1: Insert data
        print("\n[1] Insert 8 rows")
        try:
            self.sm.insert_rows("mahasiswa", test_data)
            self.assert_true(True, "Should insert rows successfully")
        except Exception as e:
            self.assert_true(False, f"Insert should succeed: {e}")

        # Test 2: Read all data
        print("\n[2] Read all data (no filter, no projection)")
        retrieval = DataRetrieval(table="mahasiswa", column=[], conditions=[])
        rows = self.sm.read_block(retrieval)
        self.assert_equal(len(rows), 8, "Should return all 8 rows")

        # Test 3: Projection (select specific columns)
        print("\n[3] Projection - select only nim, nama, ipk")
        retrieval = DataRetrieval(table="mahasiswa", column=["nim", "nama", "ipk"], conditions=[])
        rows = self.sm.read_block(retrieval)
        self.assert_equal(len(rows), 8, "Should return 8 rows")
        if len(rows) > 0:
            self.assert_equal(len(rows[0]), 3, "Each row should have 3 columns")
            self.assert_true("angkatan" not in rows[0], "Should not include 'angkatan'")

        # Test 4: Filter - single condition (ipk >= 3.5)
        print("\n[4] Filter - WHERE ipk >= 3.5")
        kondisi = Condition(column="ipk", operation=">=", operand=3.5)
        retrieval = DataRetrieval(table="mahasiswa", column=[], conditions=[kondisi])
        rows = self.sm.read_block(retrieval)
        self.assert_equal(len(rows), 5, "Should return 5 rows with ipk >= 3.5")

        # Test 5: Filter - multiple conditions (AND logic)
        print("\n[5] Filter - WHERE ipk >= 3.5 AND angkatan = 2021")
        kondisi_ipk = Condition(column="ipk", operation=">=", operand=3.5)
        kondisi_angkatan = Condition(column="angkatan", operation="=", operand=2021)
        retrieval = DataRetrieval(
            table="mahasiswa",
            column=["nim", "nama", "ipk", "angkatan"],
            conditions=[kondisi_ipk, kondisi_angkatan]
        )
        rows = self.sm.read_block(retrieval)
        self.assert_equal(len(rows), 2, "Should return 2 rows")

        # Test 6: Filter - string equality
        print("\n[6] Filter - WHERE jurusan = 'IF'")
        kondisi = Condition(column="jurusan", operation="=", operand="IF")
        retrieval = DataRetrieval(table="mahasiswa", column=[], conditions=[kondisi])
        rows = self.sm.read_block(retrieval)
        self.assert_equal(len(rows), 5, "Should return 5 rows with jurusan='IF'")

        # Test 7: Filter - no match
        print("\n[7] Filter - WHERE ipk > 4.0 (no match)")
        kondisi = Condition(column="ipk", operation=">", operand=4.0)
        retrieval = DataRetrieval(table="mahasiswa", column=[], conditions=[kondisi])
        rows = self.sm.read_block(retrieval)
        self.assert_equal(len(rows), 0, "Should return 0 rows (no match)")

        # Test 8: Filter - less than
        print("\n[8] Filter - WHERE ipk < 3.3")
        kondisi = Condition(column="ipk", operation="<", operand=3.3)
        retrieval = DataRetrieval(table="mahasiswa", column=["nim", "nama", "ipk"], conditions=[kondisi])
        rows = self.sm.read_block(retrieval)
        self.assert_equal(len(rows), 2, "Should return 2 rows with ipk < 3.3")

        # Test 9: Filter - not equal
        print("\n[9] Filter - WHERE jurusan <> 'IF'")
        kondisi = Condition(column="jurusan", operation="<>", operand="IF")
        retrieval = DataRetrieval(table="mahasiswa", column=[], conditions=[kondisi])
        rows = self.sm.read_block(retrieval)
        self.assert_equal(len(rows), 3, "Should return 3 rows with jurusan != 'IF'")

        # Test 10: Complex filter
        print("\n[10] Complex filter - WHERE ipk > 3.0 AND jurusan = 'STI' AND angkatan >= 2021")
        kondisi_ipk = Condition(column="ipk", operation=">", operand=3.0)
        kondisi_jurusan = Condition(column="jurusan", operation="=", operand="STI")
        kondisi_angkatan = Condition(column="angkatan", operation=">=", operand=2021)
        retrieval = DataRetrieval(
            table="mahasiswa",
            column=[],
            conditions=[kondisi_ipk, kondisi_jurusan, kondisi_angkatan]
        )
        rows = self.sm.read_block(retrieval)
        self.assert_equal(len(rows), 2, "Should return 2 rows")

    # ========== Test: write_block (TODO) ==========

    def test_write_block(self):
        """Test write_block (INSERT dan UPDATE) data ke tabel 'employee'."""
        self.print_header("write_block (INSERT & UPDATE) ")
        
        TABLE_NAME = "employee"

        # buat tabel testing baru 'employee'
        employee_cols = [
            ColumnDefinition("id_pegawai", "INTEGER", is_primary_key=True, is_nullable=False),
            ColumnDefinition("nama", "VARCHAR", size=50, is_nullable=False),
            ColumnDefinition("gaji", "FLOAT", is_nullable=True),
            ColumnDefinition("departemen", "VARCHAR", size=20, default_value="Marketing"),
        ]
        
        # Buat tabel jika belum ada (menjaga independensi test)
        if TABLE_NAME not in self.sm.tables:
            self.sm.create_table(TABLE_NAME, employee_cols)
        
        # Insert data awal (pra-syarat untuk operasi UPDATE)
        initial_data = [
            {"id_pegawai": 101, "nama": "Asep Setiawan", "gaji": 5000000.0, "departemen": "IT"},
            {"id_pegawai": 102, "nama": "Bunga Citra", "gaji": 6500000.0, "departemen": "Finance"},
        ]
        try:
            self.sm.insert_rows(TABLE_NAME, initial_data)
            self.assert_true(True, f"Pre-syarat: Insert {len(initial_data)} baris awal berhasil.")
        except Exception as e:
            self.assert_true(False, f"Gagal menyiapkan data awal: {e}")
            return
            
        # =========================================================
        # SKENARIO 1: INSERT DATA BARU (write_block tanpa conditions)
        # =========================================================
        print("\n[1] INSERT DATA BARU (tanpa conditions)")
        
        # INSERT row baru: ID 103, Nama "Cahya Dewi", Gaji 4800000.0. Departemen menggunakan nilai default "Marketing".
        insert_data_write = DataWrite(
            table=TABLE_NAME,
            column=["id_pegawai", "nama", "gaji"],
            new_value=[103, "Cahya Dewi", 4800000.0],
            conditions=[] # Conditions kosong -> operasi INSERT
        )

        try:
            affected_rows = self.sm.write_block(insert_data_write)
            self.assert_equal(affected_rows, 1, "Affected rows untuk INSERT harus 1")
            
            # Verifikasi data setelah INSERT (total harus 3 baris)
            all_data = self.sm.read_block(DataRetrieval(table=TABLE_NAME))
            self.assert_equal(len(all_data), 3, "Jumlah baris setelah INSERT harus 3")
            
            cahya_row = next((row for row in all_data if row["id_pegawai"] == 103), None)
            self.assert_true(cahya_row is not None, "Baris Cahya Dewi ditemukan")
            if cahya_row:
                self.assert_equal(cahya_row["departemen"], "Marketing", "Departemen Cahya (nilai default terisi)")

        except Exception as e:
            self.assert_true(False, f"Test INSERT (write_block) gagal: {e}")
            
        # =========================================================
        # SKENARIO 2: UPDATE DATA (write_block dengan conditions)
        # =========================================================
        print("\n[2] UPDATE DATA (dengan conditions)")
        
        # UPDATE data: Ubah Gaji Asep (id=101) menjadi 5500000.0 dan Departemennya menjadi "R&D"
        update_conditions = [Condition(column="id_pegawai", operation="=", operand=101)]
        update_data_write = DataWrite(
            table=TABLE_NAME,
            column=["gaji", "departemen"],
            new_value=[5500000.0, "R&D"],
            conditions=update_conditions # Conditions ada -> operasi UPDATE
        )

        try:
            affected_rows = self.sm.write_block(update_data_write)
            self.assert_equal(affected_rows, 1, "Affected rows untuk UPDATE harus 1")
            
            # Verifikasi data setelah UPDATE
            data_asep = self.sm.read_block(
                DataRetrieval(
                    table=TABLE_NAME,
                    conditions=update_conditions
                )
            )
            
            self.assert_equal(len(data_asep), 1, "Jumlah baris Asep yang cocok setelah UPDATE")
            
            if data_asep:
                asep_row = data_asep[0]
                self.assert_equal(asep_row["gaji"], 5500000.0, "Gaji Asep setelah UPDATE harus 5500000.0")
                self.assert_equal(asep_row["departemen"], "R&D", "Departemen Asep setelah UPDATE harus R&D")
                
        except Exception as e:
            self.assert_true(False, f"Test UPDATE (write_block) gagal: {e}")

        # =========================================================
        # SKENARIO 3: UPDATE DATA (tidak ada yang cocok)
        # =========================================================
        print("\n[3] UPDATE DATA (tidak ada yang cocok)")

        # Coba update row id_pegawai=999 (tidak ada)
        no_match_update = DataWrite(
            table=TABLE_NAME,
            column=["gaji"],
            new_value=[10000000.0],
            conditions=[Condition(column="id_pegawai", operation="=", operand=999)]
        )

        try:
            affected_rows = self.sm.write_block(no_match_update)
            self.assert_equal(affected_rows, 0, "UPDATE tanpa baris yang cocok mengembalikan 0 baris terpengaruh")
        except Exception as e:
            self.assert_true(False, f"Test UPDATE (no match) gagal: {e}")

    # ========== Test: delete_block ==========


    def test_delete_block(self):
        self.print_header("DELETE BLOCK")
        """Test delete_block functionality."""
        print("\n[1] DELETE - Menghapus mahasiswa dengan IPK < 3.3")
        try:
            kondisi = Condition(column='ipk', operation='<', operand=3.3)
            data_deletion = DataDeletion(
                table='mahasiswa',
                conditions=[kondisi]
            )
            affected_rows = self.sm.delete_block(data_deletion)
            self.assert_equal(affected_rows, 2, "Should delete 2 rows with ipk < 3.3")
        except Exception as e:
            self.assert_true(False, f"DELETE should succeed: {e}")

        print("\n[2] DELETE dengan multiple conditions (WHERE ipk < 3.5 AND angkatan < 2021)")
        try:
            kondisi_ipk = Condition(column='ipk', operation='<', operand=3.5)
            kondisi_angkatan = Condition(column='angkatan', operation='<', operand=2021)
            data_deletion = DataDeletion(
                table='mahasiswa',
                conditions=[kondisi_ipk, kondisi_angkatan]
            )
            affected_rows = self.sm.delete_block(data_deletion)
            self.assert_equal(affected_rows, 0, "Should delete 0 row matching both conditions")
        except Exception as e:
            self.assert_true(False, f"DELETE should succeed: {e}")

    # ========== Test: set_index (TODO) ==========

    def test_set_index(self):
        """Test set_index functionality."""
        self.print_header("SET INDEX (TODO)")
        print("\n[INFO] set_index belum diimplementasi")
        print("  TODO: Test create hash index")
        print("  TODO: Test create B+ tree index")
        print("  TODO: Test index on different column types")

    # ========== Test: get_stats ==========

    def test_get_stats(self):
        """Test get_stats functionality."""
        self.print_header("GET STATS")

        # Setup: Create table untuk testing statistik
        TABLE_NAME = "stats_test"

        if TABLE_NAME in self.sm.tables:
            # Skip jika sudah ada
            pass
        else:
            stats_cols = [
                ColumnDefinition("id", "INTEGER", is_primary_key=True),
                ColumnDefinition("name", "VARCHAR", size=50),
                ColumnDefinition("score", "FLOAT"),
                ColumnDefinition("category", "VARCHAR", size=20)
            ]
            self.sm.create_table(TABLE_NAME, stats_cols)

        # Test 1: Empty table stats
        print("\n[1] Test statistik tabel kosong")
        try:
            stats = self.sm.get_stats()
            if TABLE_NAME in stats:
                table_stats = stats[TABLE_NAME]
                self.assert_equal(table_stats.n_r, 0, "n_r untuk tabel kosong harus 0")
                self.assert_equal(table_stats.b_r, 0, "b_r untuk tabel kosong harus 0")
            else:
                self.assert_true(False, f"Tabel '{TABLE_NAME}' tidak ditemukan di stats")
        except Exception as e:
            self.assert_true(False, f"get_stats gagal untuk tabel kosong: {e}")

        # Insert test data
        test_data = [
            {"id": 1, "name": "Alice", "score": 85.5, "category": "A"},
            {"id": 2, "name": "Bob", "score": 90.0, "category": "A"},
            {"id": 3, "name": "Charlie", "score": 75.0, "category": "B"},
            {"id": 4, "name": "Diana", "score": 88.5, "category": "A"},
            {"id": 5, "name": "Eve", "score": 92.0, "category": "A"},
            {"id": 6, "name": "Frank", "score": 70.0, "category": "C"},
            {"id": 7, "name": "Grace", "score": 85.5, "category": "B"},
            {"id": 8, "name": "Henry", "score": 78.0, "category": "B"},
            {"id": 9, "name": "Ivy", "score": 95.0, "category": "A"},
            {"id": 10, "name": "Jack", "score": 82.0, "category": "B"},
        ]
        self.sm.insert_rows(TABLE_NAME, test_data)

        # Test 2: n_r (number of tuples)
        print("\n[2] Test n_r (jumlah tuple)")
        try:
            stats = self.sm.get_stats()
            table_stats = stats[TABLE_NAME]
            self.assert_equal(table_stats.n_r, 10, "n_r harus 10 (jumlah rows yang diinsert)")
        except Exception as e:
            self.assert_true(False, f"Test n_r gagal: {e}")

        # Test 3: b_r (number of blocks)
        print("\n[3] Test b_r (jumlah blok)")
        try:
            stats = self.sm.get_stats()
            table_stats = stats[TABLE_NAME]
            self.assert_true(table_stats.b_r >= 1, f"b_r harus >= 1, got {table_stats.b_r}")
            print(f"    Info: b_r = {table_stats.b_r} blok")
        except Exception as e:
            self.assert_true(False, f"Test b_r gagal: {e}")

        # Test 4: l_r (average tuple size)
        print("\n[4] Test l_r (rata-rata ukuran tuple)")
        try:
            stats = self.sm.get_stats()
            table_stats = stats[TABLE_NAME]
            self.assert_true(table_stats.l_r > 0, f"l_r harus > 0, got {table_stats.l_r}")
            print(f"    Info: l_r = {table_stats.l_r} bytes")
        except Exception as e:
            self.assert_true(False, f"Test l_r gagal: {e}")

        # Test 5: f_r (blocking factor)
        print("\n[5] Test f_r (blocking factor)")
        try:
            stats = self.sm.get_stats()
            table_stats = stats[TABLE_NAME]
            self.assert_true(table_stats.f_r >= 1, f"f_r harus >= 1, got {table_stats.f_r}")
            print(f"    Info: f_r = {table_stats.f_r} tuples/block")
        except Exception as e:
            self.assert_true(False, f"Test f_r gagal: {e}")

        # Test 6: V(A,r) - distinct values per attribute
        print("\n[6] Test V(A,r) - distinct values per attribute")
        try:
            stats = self.sm.get_stats()
            table_stats = stats[TABLE_NAME]

            # id harus 10 distinct (semua unik)
            self.assert_equal(table_stats.V_a_r.get("id", 0), 10, "V(id,r) harus 10 (semua id unik)")

            # name harus 10 distinct (semua unik)
            self.assert_equal(table_stats.V_a_r.get("name", 0), 10, "V(name,r) harus 10 (semua nama unik)")

            # score: Alice=85.5, Grace=85.5 (duplikat), jadi 9 distinct
            self.assert_equal(table_stats.V_a_r.get("score", 0), 9, "V(score,r) harus 9 (ada 1 duplikat)")

            # category: A=5, B=4, C=1 -> 3 distinct
            self.assert_equal(table_stats.V_a_r.get("category", 0), 3, "V(category,r) harus 3 (A, B, C)")

        except Exception as e:
            self.assert_true(False, f"Test V(A,r) gagal: {e}")

        # Test 7: Verifikasi rumus b_r = ceil(n_r / f_r)
        print("\n[7] Verifikasi rumus b_r = ceil(n_r / f_r)")
        try:
            import math
            stats = self.sm.get_stats()
            table_stats = stats[TABLE_NAME]

            if table_stats.f_r > 0:
                calculated_br = math.ceil(table_stats.n_r / table_stats.f_r)
                # Note: actual b_r mungkin berbeda karena cara data di-pack ke blocks
                self.assert_true(
                    table_stats.b_r <= calculated_br + 1,
                    f"b_r ({table_stats.b_r}) harus mendekati ceil(n_r/f_r) ({calculated_br})"
                )
                print(f"    Info: ceil({table_stats.n_r}/{table_stats.f_r}) = {calculated_br}, actual b_r = {table_stats.b_r}")
        except Exception as e:
            self.assert_true(False, f"Verifikasi rumus gagal: {e}")

        # Test 8: Multiple tables stats
        print("\n[8] Test statistik multiple tables")
        try:
            # Buat tabel kedua untuk test
            if "stats_test_2" not in self.sm.tables:
                self.sm.create_table("stats_test_2", [
                    ColumnDefinition("id", "INTEGER"),
                    ColumnDefinition("value", "VARCHAR", size=20)
                ])
                self.sm.insert_rows("stats_test_2", [
                    {"id": 1, "value": "test1"},
                    {"id": 2, "value": "test2"}
                ])

            stats = self.sm.get_stats()
            self.assert_true(len(stats) >= 2, f"Harus ada >= 2 tabel di stats, got {len(stats)}")
            print(f"    Info: {len(stats)} tabel ditemukan: {list(stats.keys())}")

            # Verifikasi tabel kedua juga punya stats yang benar
            if "stats_test_2" in stats:
                self.assert_equal(stats["stats_test_2"].n_r, 2, "stats_test_2 harus punya n_r = 2")
        except Exception as e:
            self.assert_true(False, f"Test multiple tables stats gagal: {e}")

        # Test 9: Large dataset dengan multiple blocks
        print("\n[9] Test dengan dataset besar (1000 rows, multiple blocks)")
        try:
            import random

            LARGE_TABLE = "large_stats_test"
            if LARGE_TABLE not in self.sm.tables:
                self.sm.create_table(LARGE_TABLE, [
                    ColumnDefinition("id", "INTEGER", is_primary_key=True),
                    ColumnDefinition("name", "VARCHAR", size=100),
                    ColumnDefinition("email", "VARCHAR", size=100),
                    ColumnDefinition("age", "INTEGER"),
                    ColumnDefinition("salary", "FLOAT"),
                    ColumnDefinition("department", "VARCHAR", size=30),
                    ColumnDefinition("is_active", "INTEGER")
                ])

            # Generate 1000 rows dengan data yang bervariasi
            departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "IT", "Legal"]
            large_data = []
            for i in range(1000):
                large_data.append({
                    "id": i + 1,
                    "name": f"Employee_{i+1:04d}",
                    "email": f"employee{i+1}@company.com",
                    "age": 22 + (i % 43),  # age 22-64 (43 distinct)
                    "salary": 30000.0 + (i % 100) * 1000,  # 100 distinct salaries
                    "department": departments[i % len(departments)],  # 8 distinct
                    "is_active": i % 2  # 2 distinct (0 or 1)
                })

            self.sm.insert_rows(LARGE_TABLE, large_data)

            stats = self.sm.get_stats()
            large_stats = stats[LARGE_TABLE]

            # Test n_r
            self.assert_equal(large_stats.n_r, 1000, "n_r harus 1000")

            # Test b_r (harus > 1 untuk 1000 rows)
            self.assert_true(large_stats.b_r >= 1, f"b_r harus >= 1, got {large_stats.b_r}")
            print(f"    Info: n_r = {large_stats.n_r}, b_r = {large_stats.b_r} blok")

            # Test l_r dan f_r
            self.assert_true(large_stats.l_r > 0, f"l_r harus > 0, got {large_stats.l_r}")
            self.assert_true(large_stats.f_r >= 1, f"f_r harus >= 1, got {large_stats.f_r}")
            print(f"    Info: l_r = {large_stats.l_r} bytes, f_r = {large_stats.f_r} tuples/block")

            # Test V(A,r) untuk berbagai kolom
            self.assert_equal(large_stats.V_a_r.get("id", 0), 1000, "V(id,r) harus 1000 (semua unik)")
            self.assert_equal(large_stats.V_a_r.get("department", 0), 8, "V(department,r) harus 8")
            self.assert_equal(large_stats.V_a_r.get("is_active", 0), 2, "V(is_active,r) harus 2")
            self.assert_equal(large_stats.V_a_r.get("age", 0), 43, "V(age,r) harus 43")
            self.assert_equal(large_stats.V_a_r.get("salary", 0), 100, "V(salary,r) harus 100")

            print(f"    Info: V(id)={large_stats.V_a_r.get('id')}, V(dept)={large_stats.V_a_r.get('department')}, V(age)={large_stats.V_a_r.get('age')}, V(salary)={large_stats.V_a_r.get('salary')}")

            # Verifikasi rumus b_r untuk large dataset
            import math
            if large_stats.f_r > 0:
                calculated_br = math.ceil(large_stats.n_r / large_stats.f_r)
                print(f"    Info: ceil({large_stats.n_r}/{large_stats.f_r}) = {calculated_br}, actual b_r = {large_stats.b_r}")

        except Exception as e:
            self.assert_true(False, f"Test large dataset gagal: {e}")

    # ========== Test Runner ==========

    def run_all_tests(self):
        """Run all tests."""
        self.setup()

        self.test_create_table()
        self.test_insert_and_read()
        self.test_write_block()
        self.test_delete_block()
        self.test_set_index()
        self.test_get_stats()

        self.teardown()
        self.print_summary()

        return self.failed == 0


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Test Storage Manager")
    parser.add_argument(
        "--test",
        type=str,
        help="Run specific test (e.g., create_table, read_block, etc.)",
        default=None
    )

    args = parser.parse_args()

    tester = TestStorageManager()

    if args.test:
        # Run specific test
        test_method = f"test_{args.test}"
        if hasattr(tester, test_method):
            tester.setup()
            getattr(tester, test_method)()
            tester.teardown()
            tester.print_summary()
        else:
            print(f"Error: Test '{args.test}' not found")
            print(f"Available tests: create_table, insert_and_read, write_block, delete_block, set_index, get_stats")
            sys.exit(1)
    else:
        # Run all tests
        success = tester.run_all_tests()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
