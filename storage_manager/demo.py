#!/usr/bin/env python3
"""demo interaktif storage manager - read, write, delete operations"""

from .storage_manager import StorageManager
from .models import (
    ColumnDefinition,
    DataRetrieval,
    DataWrite,
    DataDeletion,
    Condition
)

def print_separator():
    print("\n" + "="*70 + "\n")

def print_rows(rows, title="hasil:"):
    """print rows dalam format tabel simple"""
    print(f"\n{title}")
    print("-" * 70)
    if not rows:
        print("(kosong)")
        return

    # ambil kolom dari row pertama
    if rows:
        cols = list(rows[0].keys())
        # print header
        header = " | ".join(f"{col:15}" for col in cols)
        print(header)
        print("-" * len(header))

        # print data
        for row in rows:
            row_str = " | ".join(f"{str(row.get(col, 'NULL')):15}" for col in cols)
            print(row_str)
    print("-" * 70)

def main():
    print("🎯 DEMO STORAGE MANAGER - READ/WRITE/DELETE")
    print_separator()

    # 1. init storage manager
    print("📦 [1] INISIALISASI STORAGE MANAGER")
    sm = StorageManager(data_dir="demo_data")
    print_separator()

    # 2. create table mahasiswa
    print("📝 [2] CREATE TABLE mahasiswa")
    columns = [
        ColumnDefinition("nim", "VARCHAR", size=20, is_primary_key=True, is_nullable=False),
        ColumnDefinition("nama", "VARCHAR", size=50, is_nullable=False),
        ColumnDefinition("ipk", "FLOAT", is_nullable=False),
        ColumnDefinition("angkatan", "INTEGER", is_nullable=False),
        ColumnDefinition("status", "VARCHAR", size=20, default_value="Aktif")
    ]
    sm.create_table("mahasiswa", columns, primary_keys=["nim"])
    print_separator()

    # 3. insert data menggunakan write_block
    print("➕ [3] INSERT DATA (write_block)")
    print("inserting 5 mahasiswa...")

    data_mahasiswa = [
        ("13521001", "Budi Santoso", 3.75, 2021),
        ("13521002", "Ani Wijaya", 3.90, 2021),
        ("13522001", "Citra Dewi", 3.25, 2022),
        ("13522002", "Doni Pratama", 3.50, 2022),
        ("13523001", "Eka Putri", 3.85, 2023),
    ]

    for nim, nama, ipk, angkatan in data_mahasiswa:
        write_req = DataWrite(
            table="mahasiswa",
            column=["nim", "nama", "ipk", "angkatan"],
            new_value=[nim, nama, ipk, angkatan],
            conditions=[]  # no conditions = INSERT
        )
        sm.write_block(write_req)

    print_separator()

    # 4. read all data
    print("📖 [4] READ ALL DATA")
    read_req = DataRetrieval(table="mahasiswa", column=[], conditions=[])
    rows = sm.read_block(read_req)
    print_rows(rows, f"semua mahasiswa ({len(rows)} rows):")
    print_separator()

    # 5. read dengan filter
    print("🔍 [5] READ DENGAN FILTER (ipk >= 3.5)")
    kondisi_ipk = Condition(column="ipk", operation=">=", operand=3.5)
    read_req = DataRetrieval(
        table="mahasiswa",
        column=[],
        conditions=[kondisi_ipk]
    )
    rows = sm.read_block(read_req)
    print_rows(rows, f"mahasiswa dengan ipk >= 3.5 ({len(rows)} rows):")
    print_separator()

    # 6. read dengan projection
    print("📊 [6] READ DENGAN PROJECTION (hanya nim, nama, ipk)")
    read_req = DataRetrieval(
        table="mahasiswa",
        column=["nim", "nama", "ipk"],
        conditions=[]
    )
    rows = sm.read_block(read_req)
    print_rows(rows, f"projection nim, nama, ipk ({len(rows)} rows):")
    print_separator()

    # 7. read dengan filter dan projection
    print("🎯 [7] FILTER + PROJECTION (angkatan=2022, tampil nim+nama saja)")
    kondisi_angkatan = Condition(column="angkatan", operation="=", operand=2022)
    read_req = DataRetrieval(
        table="mahasiswa",
        column=["nim", "nama"],
        conditions=[kondisi_angkatan]
    )
    rows = sm.read_block(read_req)
    print_rows(rows, f"mahasiswa angkatan 2022 ({len(rows)} rows):")
    print_separator()

    # 8. update data
    print("✏️  [8] UPDATE DATA (status mahasiswa angkatan 2021 jadi 'Lulus')")
    kondisi_angkatan = Condition(column="angkatan", operation="=", operand=2021)
    update_req = DataWrite(
        table="mahasiswa",
        column=["status"],
        new_value=["Lulus"],
        conditions=[kondisi_angkatan]
    )
    affected = sm.write_block(update_req)
    print(f"✓ updated {affected} rows")

    # show hasil update
    read_req = DataRetrieval(table="mahasiswa", column=[], conditions=[])
    rows = sm.read_block(read_req)
    print_rows(rows, "setelah update:")
    print_separator()

    # 9. delete data
    print("🗑️  [9] DELETE DATA (hapus mahasiswa dengan ipk < 3.3)")
    kondisi_ipk = Condition(column="ipk", operation="<", operand=3.3)
    delete_req = DataDeletion(
        table="mahasiswa",
        conditions=[kondisi_ipk]
    )
    deleted = sm.delete_block(delete_req)
    print(f"✓ deleted {deleted} rows")

    # show hasil delete
    read_req = DataRetrieval(table="mahasiswa", column=[], conditions=[])
    rows = sm.read_block(read_req)
    print_rows(rows, f"setelah delete ({len(rows)} rows tersisa):")
    print_separator()

    # 10. complex query
    print("🔥 [10] COMPLEX QUERY (ipk >= 3.7 AND status = 'Aktif')")
    kondisi_ipk = Condition(column="ipk", operation=">=", operand=3.7)
    kondisi_status = Condition(column="status", operation="=", operand="Aktif")
    read_req = DataRetrieval(
        table="mahasiswa",
        column=["nim", "nama", "ipk", "status"],
        conditions=[kondisi_ipk, kondisi_status]
    )
    rows = sm.read_block(read_req)
    print_rows(rows, f"mahasiswa ipk tinggi yang masih aktif ({len(rows)} rows):")
    print_separator()

    print("✅ DEMO SELESAI!")
    print("📁 check 'demo_data/' untuk lihat binary files yang dibuat")
    print()

if __name__ == "__main__":
    main()
