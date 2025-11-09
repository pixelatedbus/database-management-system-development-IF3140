"""
File demonstrasi cara menggunakan Storage Manager.

Contoh penggunaan untuk:
- Membuat kondisi filtering
- Membaca data (read_block)
- Menulis data (write_block)
- Menghapus data (delete_block)
- Membuat index (set_index)
- Mengambil statistik (get_stats)
"""

from storage_manager import (
    Condition,
    DataRetrieval,
    DataWrite,
    DataDeletion,
    Statistic,
    StorageManager,
)


def demo_basic_usage():
    """Demonstrasi penggunaan dasar Storage Manager."""
    
    # 1. Inisialisasi Storage Manager
    print("=" * 60)
    print("1. INISIALISASI STORAGE MANAGER")
    print("=" * 60)
    sm = StorageManager()
    print("✓ Storage Manager berhasil diinisialisasi\n")
    
    
    # 2. Contoh READ BLOCK - Membaca data dari tabel
    print("=" * 60)
    print("2. READ BLOCK - Membaca Data")
    print("=" * 60)
    
    # Membaca semua data dari tabel mahasiswa
    print("\nContoh 1: Membaca semua data dari tabel 'mahasiswa'")
    data_retrieval = DataRetrieval(
        table="mahasiswa",
        column=["nim", "nama", "ipk"],  # kolom yang ingin diambil
        conditions=[]  # tanpa kondisi = ambil semua
    )
    print(f"  table: {data_retrieval.table}")
    print(f"  column: {data_retrieval.column}")
    print(f"  conditions: {data_retrieval.conditions}")
    # rows = sm.read_block(data_retrieval)  # akan error karena belum diimplementasi
    
    
    # Membaca data dengan kondisi WHERE
    print("\nContoh 2: Membaca data dengan kondisi WHERE ipk >= 3.5")
    kondisi_ipk = Condition(
        column="ipk",
        operation=">=",
        operand=3.5
    )
    data_retrieval_filtered = DataRetrieval(
        table="mahasiswa",
        column=["nim", "nama", "ipk"],
        conditions=[kondisi_ipk]
    )
    print(f"  Kondisi: {kondisi_ipk.column} {kondisi_ipk.operation} {kondisi_ipk.operand}")
    # rows = sm.read_block(data_retrieval_filtered)
    
    
    # Membaca dengan multiple conditions (AND)
    print("\nContoh 3: Membaca dengan multiple conditions (AND)")
    kondisi_ipk = Condition(column="ipk", operation=">=", operand=3.5)
    kondisi_angkatan = Condition(column="angkatan", operation="=", operand=2021)
    
    data_retrieval_multi = DataRetrieval(
        table="mahasiswa",
        column=["nim", "nama", "ipk", "angkatan"],
        conditions=[kondisi_ipk, kondisi_angkatan]  # akan di-AND
    )
    print(f"  Kondisi 1: ipk >= 3.5")
    print(f"  Kondisi 2: angkatan = 2021")
    # rows = sm.read_block(data_retrieval_multi)
    print()
    
    
    # 3. Contoh WRITE BLOCK - Insert data baru
    print("=" * 60)
    print("3. WRITE BLOCK - Insert/Update Data")
    print("=" * 60)
    
    # INSERT: Menambah mahasiswa baru
    print("\nContoh 1: INSERT - Menambah mahasiswa baru")
    data_write_insert = DataWrite(
        table="mahasiswa",
        column=["nim", "nama", "ipk", "angkatan"],
        conditions=[],  # kosong untuk INSERT
        new_value=["13520001", "Budi Santoso", 3.75, 2021]
    )
    print(f"  table: {data_write_insert.table}")
    print(f"  column: {data_write_insert.column}")
    print(f"  new_value: {data_write_insert.new_value}")
    # affected_rows = sm.write_block(data_write_insert)
    # print(f"Berhasil insert {affected_rows} baris")
    
    
    # UPDATE: Mengubah data mahasiswa
    print("\nContoh 2: UPDATE - Mengubah IPK mahasiswa dengan NIM tertentu")
    kondisi_nim = Condition(column="nim", operation="=", operand="13520001")
    
    data_write_update = DataWrite(
        table="mahasiswa",
        column=["ipk"],  # kolom yang akan diupdate
        conditions=[kondisi_nim],  # WHERE nim = '13520001'
        new_value=[3.85]  # nilai baru untuk IPK
    )
    print(f"  UPDATE mahasiswa SET ipk = 3.85 WHERE nim = '13520001'")
    # affected_rows = sm.write_block(data_write_update)
    # print(f"Berhasil update {affected_rows} baris")
    print()
    
    
    # 4. Contoh DELETE BLOCK - Menghapus data
    print("=" * 60)
    print("4. DELETE BLOCK - Menghapus Data")
    print("=" * 60)
    
    # DELETE dengan kondisi
    print("\nContoh 1: DELETE - Menghapus mahasiswa dengan IPK < 2.0")
    kondisi_delete = Condition(column="ipk", operation="<", operand=2.0)
    
    data_deletion = DataDeletion(
        table="mahasiswa",
        conditions=[kondisi_delete]
    )
    print(f"  DELETE FROM mahasiswa WHERE ipk < 2.0")
    # deleted_rows = sm.delete_block(data_deletion)
    # print(f"Berhasil hapus {deleted_rows} baris")
    
    
    # DELETE dengan multiple conditions
    print("\nContoh 2: DELETE dengan multiple conditions")
    kondisi_ipk = Condition(column="ipk", operation="<", operand=2.0)
    kondisi_angkatan = Condition(column="angkatan", operation="<", operand=2020)
    
    data_deletion_multi = DataDeletion(
        table="mahasiswa",
        conditions=[kondisi_ipk, kondisi_angkatan]
    )
    print(f"  DELETE FROM mahasiswa WHERE ipk < 2.0 AND angkatan < 2020")
    # deleted_rows = sm.delete_block(data_deletion_multi)
    print()
    
    
    # 5. Contoh SET INDEX - Membuat index
    print("=" * 60)
    print("5. SET INDEX - Membuat Index")
    print("=" * 60)
    
    print("\nContoh 1: Membuat Hash Index pada kolom 'nim'")
    # sm.set_index(table="mahasiswa", column="nim", index_type="hash")
    print(f"  ✓ Hash index berhasil dibuat pada mahasiswa(nim)")
    
    print("\nContoh 2: Membuat B+ Tree Index pada kolom 'ipk'")
    # sm.set_index(table="mahasiswa", column="ipk", index_type="btree")
    print(f"  ✓ B+ Tree index berhasil dibuat pada mahasiswa(ipk)")
    print()
    
    
    # 6. Contoh GET STATS - Mengambil statistik
    print("=" * 60)
    print("6. GET STATS - Mengambil Statistik")
    print("=" * 60)
    
    print("\nMengambil statistik untuk semua tabel:")
    # stats = sm.get_stats()
    # for table_name, stat in stats.items():
    #     print(f"\n  Tabel: {table_name}")
    #     print(f"    - n_r (jumlah tuple): {stat.n_r}")
    #     print(f"    - b_r (jumlah blok): {stat.b_r}")
    #     print(f"    - l_r (ukuran tuple): {stat.l_r} bytes")
    #     print(f"    - f_r (blocking factor): {stat.f_r}")
    #     print(f"    - V_a_r (distinct values):")
    #     for attr, count in stat.V_a_r.items():
    #         print(f"        {attr}: {count} distinct values")
    
    # Contoh output yang diharapkan:
    print("\n  Contoh output yang diharapkan:")
    print("  Tabel: mahasiswa")
    print("    - n_r (jumlah tuple): 150")
    print("    - b_r (jumlah blok): 5")
    print("    - l_r (ukuran tuple): 128 bytes")
    print("    - f_r (blocking factor): 32")
    print("    - V_a_r (distinct values):")
    print("        nim: 150 distinct values")
    print("        nama: 148 distinct values")
    print("        ipk: 45 distinct values")
    print("        angkatan: 4 distinct values")
    print()


def demo_operators():
    """Demonstrasi berbagai operator yang didukung."""
    
    print("=" * 60)
    print("DEMONSTRASI BERBAGAI OPERATOR")
    print("=" * 60)
    print()
    
    operators = [
        ("=", "sama dengan", 3.5),
        ("<>", "tidak sama dengan", 3.0),
        ("<", "kurang dari", 3.0),
        ("<=", "kurang dari atau sama dengan", 3.5),
        (">", "lebih dari", 3.5),
        (">=", "lebih dari atau sama dengan", 3.0),
    ]
    
    for op, desc, value in operators:
        kondisi = Condition(
            column="ipk",
            operation=op,
            operand=value
        )
        print(f"  {desc:35} : ipk {op} {value}")
    print()


def demo_use_cases():
    """Demonstrasi use case praktis."""
    
    print("=" * 60)
    print("USE CASE PRAKTIS")
    print("=" * 60)
    
    # Use Case 1: Mencari mahasiswa berprestasi
    print("\nUse Case 1: Mencari mahasiswa berprestasi (IPK >= 3.7)")
    print("-" * 60)
    kondisi = Condition(column="ipk", operation=">=", operand=3.7)
    dr = DataRetrieval(
        table="mahasiswa",
        column=["nim", "nama", "ipk"],
        conditions=[kondisi]
    )
    print(f"  Query: SELECT nim, nama, ipk FROM mahasiswa WHERE ipk >= 3.7")
    print(f"  DataRetrieval object created ✓")
    
    
    # Use Case 2: Update IPK setelah semester baru
    print("\nUse Case 2: Update IPK mahasiswa setelah semester baru")
    print("-" * 60)
    kondisi = Condition(column="nim", operation="=", operand="13520001")
    dw = DataWrite(
        table="mahasiswa",
        column=["ipk"],
        conditions=[kondisi],
        new_value=[3.85]
    )
    print(f"  Query: UPDATE mahasiswa SET ipk = 3.85 WHERE nim = '13520001'")
    print(f"  DataWrite object created ✓")
    
    
    # Use Case 3: Hapus data mahasiswa yang sudah lulus
    print("\nUse Case 3: Hapus data mahasiswa angkatan lama")
    print("-" * 60)
    kondisi = Condition(column="angkatan", operation="<", operand=2015)
    dd = DataDeletion(
        table="mahasiswa",
        conditions=[kondisi]
    )
    print(f"  Query: DELETE FROM mahasiswa WHERE angkatan < 2015")
    print(f"  DataDeletion object created ✓")
    
    
    # Use Case 4: Optimisasi query dengan index
    print("\nUse Case 4: Optimisasi pencarian dengan index")
    print("-" * 60)
    print(f"  Membuat index pada kolom yang sering dicari (nim, nama)")
    print(f"  sm.set_index('mahasiswa', 'nim', 'hash')")
    print(f"  sm.set_index('mahasiswa', 'nama', 'btree')")
    print(f"  Index berhasil dibuat ✓")
    print()


if __name__ == "__main__":
    print("\n" + "="*60)
    print("DEMONSTRASI STORAGE MANAGER")
    print("="*60 + "\n")
    
    # Jalankan demonstrasi
    demo_basic_usage()
    demo_operators()
    demo_use_cases()
    
    print("\n" + "="*60)
    print("CATATAN:")
    print("="*60)
    print("""
    - Semua contoh di atas menggunakan object yang sudah dibuat
    - Method sm.read_block(), sm.write_block(), dll masih NotImplementedError
    - Tim perlu mengimplementasi method-method tersebut
    - Setelah diimplementasi, uncomment baris yang ada tanda #
    - Pastikan untuk membuat file binary untuk menyimpan data tabel
    """)
    print("\n" + "="*60)
    print("DEMO SELESAI")
    print("="*60 + "\n")
