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

from storage_manager import StorageManager
from models import Condition, DataRetrieval, DataWrite, DataDeletion


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
        """Test write_block functionality (INSERT & UPDATE)."""
        self.print_header("WRITE BLOCK (TODO)")
        print("\n[INFO] write_block belum diimplementasi")
        print("  TODO: Test INSERT operation")
        print("  TODO: Test UPDATE operation")
        print("  TODO: Test UPDATE with conditions")

    # ========== Test: delete_block ==========

    def test_delete_block(self):
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

    # ========== Test: get_stats (TODO) ==========

    def test_get_stats(self):
        """Test get_stats functionality."""
        self.print_header("GET STATS (TODO)")
        print("\n[INFO] get_stats belum diimplementasi")
        print("  TODO: Test n_r (number of tuples)")
        print("  TODO: Test b_r (number of blocks)")
        print("  TODO: Test l_r (tuple size)")
        print("  TODO: Test f_r (blocking factor)")
        print("  TODO: Test V(A,r) (distinct values)")

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
