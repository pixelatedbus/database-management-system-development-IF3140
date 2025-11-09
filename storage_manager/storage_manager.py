from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class Condition:
    """Merepresentasikan satu kondisi untuk filtering data."""
    column: str
    operation: str  # '=', '<>', '<', '<=', '>', '>='
    operand: Any  # int | str


@dataclass
class DataRetrieval:
    """Parameter untuk operasi read_block."""
    table: str  # nama tabel (satu tabel saja)
    column: List[str] = field(default_factory=list)  # list kolom yang ingin diambil (untuk proyeksi)
    conditions: List[Condition] = field(default_factory=list)  # kondisi WHERE


@dataclass
class DataWrite:
    """Parameter untuk operasi write_block (insert/update)."""
    table: str  # nama tabel (satu tabel saja)
    column: List[str] = field(default_factory=list)  # list kolom yang akan di-insert/update
    conditions: List[Condition] = field(default_factory=list)  # kondisi WHERE (untuk UPDATE)
    new_value: List[Any] = field(default_factory=list)  # list nilai baru untuk setiap kolom


@dataclass
class DataDeletion:
    """Parameter untuk operasi delete_block."""
    table: str
    conditions: List[Condition] = field(default_factory=list)


@dataclass
class Statistic:
    """Statistik untuk optimisasi query.
    
    Atribut:
        n_r: jumlah tuple dalam relasi r
        b_r: jumlah blok yang berisi tuple dari r
        l_r: ukuran tuple dari r
        f_r: blocking factor dari r (jumlah tuple yang muat dalam satu blok)
        V_a_r: dict[str, int] - jumlah nilai distinct per atribut A dalam r
    """
    n_r: int
    b_r: int
    l_r: int
    f_r: int
    V_a_r: Dict[str, int] = field(default_factory=dict)


class StorageManager:
    """Komponen Storage Manager untuk menangani penyimpanan, modifikasi, dan pengambilan data.
    
    Tanggung jawab:
    - Menyimpan data dalam binary file(s) di harddisk
    - Membaca, menulis, dan menghapus blok data
    - Mengelola indeks (B+ Tree atau Hash)
    - Menyediakan statistik untuk optimisasi query
    - Berkomunikasi dengan Failure Recovery Manager untuk menangani dirty data
    
    Catatan implementasi:
    - Pilihan satu file untuk semua tabel vs satu file per tabel diserahkan kepada tim
    - Proyeksi dan seleksi bisa dilakukan di sini atau didelegasikan ke Query Processor
    """

    def __init__(self):
        """Inisialisasi Storage Manager.
        
        TODO: Inisialisasi struktur data yang diperlukan:
        - Struktur manajemen file
        - Struktur indeks
        - Cache statistik
        - Referensi ke Failure Recovery Manager
        """
        pass

    def read_block(self, data_retrieval: DataRetrieval) -> List[Dict[str, Any]]:
        """Membaca data dari disk berdasarkan parameter retrieval.
        
        Args:
            data_retrieval: Object yang berisi nama tabel, kolom, dan kondisi
            
        Returns:
            List dari baris (setiap baris sebagai dictionary) yang sesuai kondisi
            
        TODO: Implementasi logika pengambilan data:
        - Tentukan tipe pencarian (linear scan vs index-based)
        - Baca blok yang sesuai dari disk
        - Terapkan kondisi filtering
        - Return baris yang cocok
        """
        raise NotImplementedError("read_block belum diimplementasi")

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
