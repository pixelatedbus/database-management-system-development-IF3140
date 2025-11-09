"""File demonstrasi cara menggunakan Storage Manager.

Contoh penggunaan untuk:
- Membuat kondisi filtering
- Membaca data (read_block)
- Menulis data (write_block)
- Menghapus data (delete_block)
- Membuat index (set_index)
- Mengambil statistik (get_stats)
"""

from .models import Condition, DataRetrieval, DataWrite, DataDeletion
from .storage_manager import StorageManager


def demo_read_block():
    """Contoh cara menggunakan read_block()."""
    print("\n" + "="*60)
    print("CONTOH: READ BLOCK")
    print("="*60)
    
    sm = StorageManager()
    
    # Contoh 1: Membaca semua data dari tabel
    print("\n1. Membaca semua data dari tabel 'mahasiswa':")
    print("   data_retrieval = DataRetrieval(")
    print("       table='mahasiswa',")
    print("       column=['nim', 'nama', 'ipk'],")
    print("       conditions=[]")
    print("   )")
    print("   rows = sm.read_block(data_retrieval)")
    
    # Contoh 2: Membaca dengan 1 kondisi
    print("\n2. Membaca data dengan kondisi WHERE ipk >= 3.5:")
    print("   kondisi = Condition(")
    print("       column='ipk',")
    print("       operation='>=',")
    print("       operand=3.5")
    print("   )")
    print("   data_retrieval = DataRetrieval(")
    print("       table='mahasiswa',")
    print("       column=['nim', 'nama', 'ipk'],")
    print("       conditions=[kondisi]")
    print("   )")
    print("   rows = sm.read_block(data_retrieval)")
    
    # Contoh 3: Membaca dengan multiple conditions (AND)
    print("\n3. Membaca dengan multiple conditions (WHERE ipk >= 3.5 AND angkatan = 2021):")
    print("   kondisi_ipk = Condition(column='ipk', operation='>=', operand=3.5)")
    print("   kondisi_angkatan = Condition(column='angkatan', operation='=', operand=2021)")
    print("   data_retrieval = DataRetrieval(")
    print("       table='mahasiswa',")
    print("       column=['nim', 'nama', 'ipk', 'angkatan'],")
    print("       conditions=[kondisi_ipk, kondisi_angkatan]")
    print("   )")
    print("   rows = sm.read_block(data_retrieval)  # Dikembalikan dengan AND logic")


def demo_write_block():
    """Contoh cara menggunakan write_block()."""
    print("\n" + "="*60)
    print("CONTOH: WRITE BLOCK")
    print("="*60)
    
    sm = StorageManager()
    
    # Contoh 1: INSERT
    print("\n1. INSERT - Menambah mahasiswa baru:")
    print("   data_write = DataWrite(")
    print("       table='mahasiswa',")
    print("       column=['nim', 'nama', 'ipk', 'angkatan'],")
    print("       conditions=[],")
    print("       new_value=['13520001', 'Budi Santoso', 3.75, 2021]")
    print("   )")
    print("   affected_rows = sm.write_block(data_write)")
    
    # Contoh 2: UPDATE
    print("\n2. UPDATE - Mengubah IPK mahasiswa dengan NIM tertentu:")
    print("   kondisi = Condition(column='nim', operation='=', operand='13520001')")
    print("   data_write = DataWrite(")
    print("       table='mahasiswa',")
    print("       column=['ipk'],")
    print("       conditions=[kondisi],")
    print("       new_value=[3.85]")
    print("   )")
    print("   affected_rows = sm.write_block(data_write)")


def demo_delete_block():
    """Contoh cara menggunakan delete_block()."""
    print("\n" + "="*60)
    print("CONTOH: DELETE BLOCK")
    print("="*60)
    
    sm = StorageManager()
    
    # Contoh 1: DELETE dengan 1 kondisi
    print("\n1. DELETE - Menghapus mahasiswa dengan IPK < 2.0:")
    print("   kondisi = Condition(column='ipk', operation='<', operand=2.0)")
    print("   data_deletion = DataDeletion(")
    print("       table='mahasiswa',")
    print("       conditions=[kondisi]")
    print("   )")
    print("   deleted_rows = sm.delete_block(data_deletion)")
    
    # Contoh 2: DELETE dengan multiple conditions
    print("\n2. DELETE dengan multiple conditions (WHERE ipk < 2.0 AND angkatan < 2020):")
    print("   kondisi_ipk = Condition(column='ipk', operation='<', operand=2.0)")
    print("   kondisi_angkatan = Condition(column='angkatan', operation='<', operand=2020)")
    print("   data_deletion = DataDeletion(")
    print("       table='mahasiswa',")
    print("       conditions=[kondisi_ipk, kondisi_angkatan]")
    print("   )")
    print("   deleted_rows = sm.delete_block(data_deletion)")


def demo_set_index():
    """Contoh cara menggunakan set_index()."""
    print("\n" + "="*60)
    print("CONTOH: SET INDEX")
    print("="*60)
    
    sm = StorageManager()
    
    print("\n1. Membuat Hash Index pada kolom 'nim':")
    print("   sm.set_index(table='mahasiswa', column='nim', index_type='hash')")
    
    print("\n2. Membuat B+ Tree Index pada kolom 'ipk':")
    print("   sm.set_index(table='mahasiswa', column='ipk', index_type='btree')")


def demo_get_stats():
    """Contoh cara menggunakan get_stats()."""
    print("\n" + "="*60)
    print("CONTOH: GET STATS")
    print("="*60)
    
    sm = StorageManager()
    
    print("\nMengambil statistik untuk semua tabel:")
    print("   stats = sm.get_stats()")
    print("   # Hasil: Dict[str, Statistic]")
    print("   # Key: nama tabel")
    print("   # Value: Statistic(n_r, b_r, l_r, f_r, V_a_r)")
    
    print("\nContoh output:")
    print("   stats['mahasiswa'].n_r = 150  # jumlah tuple")
    print("   stats['mahasiswa'].b_r = 5    # jumlah blok")
    print("   stats['mahasiswa'].l_r = 128  # ukuran tuple")
    print("   stats['mahasiswa'].f_r = 32   # blocking factor")
    print("   stats['mahasiswa'].V_a_r = {'nim': 150, 'nama': 148, 'ipk': 45, ...}")


def demo_operators():
    """Demonstrasi berbagai operator yang didukung."""
    print("\n" + "="*60)
    print("OPERATOR YANG DIDUKUNG")
    print("="*60)
    
    operators = [
        ("=", "sama dengan"),
        ("<>", "tidak sama dengan"),
        ("<", "kurang dari"),
        ("<=", "kurang dari atau sama dengan"),
        (">", "lebih dari"),
        (">=", "lebih dari atau sama dengan"),
    ]
    
    for op, desc in operators:
        print(f"\n{op:4} → {desc}")
        print(f'   Contoh: Condition(column="ipk", operation="{op}", operand=3.5)')


if __name__ == "__main__":
    print("\n" + "="*70)
    print("PANDUAN PENGGUNAAN STORAGE MANAGER")
    print("="*70)
    
    demo_read_block()
    demo_write_block()
    demo_delete_block()
    demo_set_index()
    demo_get_stats()
    demo_operators()
    
    print("\n" + "="*70)
    print("NOTES:")
    print("="*70)
    print("""
    - Semua contoh di atas menunjukkan cara memanggil method StorageManager
    - Implementasi method masih TODO (NotImplementedError)
    - Tim perlu mengimplementasi: read_block, write_block, delete_block, 
      set_index, get_stats
    - Setelah diimplementasi, contoh di atas bisa langsung dijalankan
    """)
    print("="*70 + "\n")
