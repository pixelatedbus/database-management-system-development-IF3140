## Storage Manager

Deskripsi singkat
-----------------
`storage_manager` adalah modul yang bertanggung jawab menyimpan dan mengambil baris data (rows) dari berkas-berkas tabel pada storage lokal. Modul ini dibuat agar mudah diintegrasikan dengan bagian lain dari DBMS (mis. Query Processor). Implementasinya saat ini membaca file data dalam format JSON (default) dan menyediakan API untuk operasi baca (read). Dukungan serialisasi biner (fixed-length rows) tersedia di modul terpisah (`binary_storage.py`) dan dapat diintegrasikan bila diperlukan.

File utama
-----------
- `storage_manager.py`  — kelas `StorageManager` (fungsi utama: `read_block`, skeleton untuk `write_block`, `delete_block`, `set_index`, `get_stats`).
- `models.py`           — dataclass untuk objek-objek transfer (mis. `DataRetrieval`, `Condition`, `DataWrite`, `DataDeletion`, `Statistic`).
- `utils.py`            — helper (mis. evaluator kondisi, validasi nama tabel, projection helper).
- `binary_storage.py`   — `BinarySerializer` untuk format biner fixed-length (terpisah dari StorageManager saat ini).
- `test_read_block.py`  — test suite untuk `read_block` (filtering/projection/operator coverage).
- `setup_database.py`   — skrip untuk membuat sample database (JSON) untuk testing.
- `convert_to_binary.py`— skrip bantu untuk mengkonversi file JSON ke format biner menggunakan `BinarySerializer`.

Kontrak singkat (contract)
--------------------------
- Input utama (untuk operasi baca): objek `DataRetrieval` yang berisi:
  - `table` (str) : nama tabel
  - `columns` (list[str] atau [`*`]) : kolom yang diminta (projection)
  - `conditions` (list[Condition]) : list kondisi (digabungkan dengan AND)

- Output: list of dict — setiap dict merepresentasikan satu baris hasil dengan pasangan {kolom: nilai}.

- Error modes:
  - `TableNotFoundError` / `ValueError` bila nama tabel tidak ada
  - `TypeError` / `ValueError` bila terjadi mismatch pada kondisi atau proyeksi
  - Saat ini operasi write/delete/index belum semuanya diimplementasi; mereka dapat melempar `NotImplementedError`.

Fungsi penting dan status implementasi
## Storage Manager

Deskripsi singkat
-----------------
`storage_manager` adalah modul yang bertanggung jawab menyimpan dan mengambil baris data (rows) dari berkas-berkas tabel pada storage lokal. Modul ini dibuat agar mudah diintegrasikan dengan bagian lain dari DBMS (mis. Query Processor). Implementasinya saat ini membaca file data dalam format JSON (default) dan menyediakan API untuk operasi baca (read). Dukungan serialisasi biner (fixed-length rows) tersedia di modul terpisah (`binary_storage.py`) dan dapat diintegrasikan bila diperlukan.

File utama
-----------
- `storage_manager.py`  — kelas `StorageManager` (fungsi utama: `read_block`, skeleton untuk `write_block`, `delete_block`, `set_index`, `get_stats`).
- `models.py`           — dataclass untuk objek-objek transfer (mis. `DataRetrieval`, `Condition`, `DataWrite`, `DataDeletion`, `Statistic`).
- `utils.py`            — helper (mis. evaluator kondisi, validasi nama tabel, projection helper).
- `binary_storage.py`   — `BinarySerializer` untuk format biner fixed-length (terpisah dari StorageManager saat ini).
- `test_read_block.py`  — test suite untuk `read_block` (filtering/projection/operator coverage).
- `setup_database.py`   — skrip untuk membuat sample database (JSON) untuk testing.
- `convert_to_binary.py`— skrip bantu untuk mengkonversi file JSON ke format biner menggunakan `BinarySerializer`.

Kontrak singkat (contract)
--------------------------
- Input utama (untuk operasi baca): objek `DataRetrieval` yang berisi:
  - `table` (str) : nama tabel
  - `columns` (list[str] atau [`*`]) : kolom yang diminta (projection)
  - `conditions` (list[Condition]) : list kondisi (digabungkan dengan AND)

- Output: list of dict — setiap dict merepresentasikan satu baris hasil dengan pasangan {kolom: nilai}.

- Error modes:
  - `TableNotFoundError` / `ValueError` bila nama tabel tidak ada
  - `TypeError` / `ValueError` bila terjadi mismatch pada kondisi atau proyeksi
  - Saat ini operasi write/delete/index belum semuanya diimplementasi; mereka dapat melempar `NotImplementedError`.

Fungsi penting dan status implementasi
-------------------------------------
Berikut ringkasan fungsi yang penting untuk diintegrasikan dari luar (mis. Query Processor):

- `StorageManager.read_block(data_retrieval: DataRetrieval) -> List[dict]`  (IMPLEMENTED, JSON default)
  - Deskripsi: Menjalankan operasi baca (SELECT) sederhana: memuat file tabel, menyaring baris berdasarkan kondisi (AND), lalu melakukan projection kolom.
  - Input: `DataRetrieval` (lihat models).
  - Output: list of rows (dict).
  - Catatan integrasi: Query Processor harus membuat `DataRetrieval` sesuai models dan hanya mengharapkan list-of-dict sebagai hasil. Tipe nilai mengikuti tipe JSON (int/float/str/boolean).

- `StorageManager.write_block(data_write: DataWrite) -> bool`  (TODO / skeleton)
  - Deskripsi yang diharapkan: Menambahkan baris baru ke tabel (INSERT) atau menimpa baris jika primary-key/update berlaku.
  - Kontrak yang direkomendasikan: `DataWrite` harus berisi `table`, `rows` (list of dict), dan `mode` (append/replace).
  - Saat ini: bukan prioritas diimplementasikan sepenuhnya — Query Processor sebaiknya menunggu implementasi ini atau menggunakan skrip bantu (`convert_to_binary.py` / menulis JSON langsung untuk integration tests).

- `StorageManager.delete_block(data_deletion: DataDeletion) -> int`  (TODO)
  - Deskripsi yang diharapkan: Hapus baris yang sesuai kondisi, kembalikan jumlah baris yang dihapus.

- `StorageManager.set_index(table_name: str, column: str, index_type: str) -> bool`  (TODO)
  - Deskripsi: Buat index sederhana pada sebuah kolom (mis. hash/index file) agar pencarian menjadi cepat. Implementasi dan format index tidak ditentukan di milestone awal.

- `StorageManager.get_stats(table_name: str) -> Statistic`  (PARTIAL)
  - Deskripsi: Kembalikan metadata sederhana seperti jumlah baris, ukuran file, kolom dan tipe. Berguna untuk query optimizer.

Detail kondisi dan operator yang didukung (untuk `read_block`)
---------------------------------------------------------
- Kondisi diwakili oleh `Condition` (lihat `models.py`). `read_block` memproses kondisi dan mendukung operator umum: `==`, `!=`, `<`, `<=`, `>`, `>=`, serta pola `LIKE` sederhana (substring). Evaluator lebih lengkap ada di `utils.evaluate_condition`.
- Semua kondisi digabungkan dengan AND (kecuali jika Query Processor melakukan kombinasi lebih kompleks sebelum memanggil StorageManager).

Contoh pemanggilan (Python)
---------------------------
Berikut contoh sederhana cara integrasi dari Query Processor:

```python
from storage_manager.storage_manager import StorageManager
from storage_manager.models import DataRetrieval, Condition

# inisialisasi StorageManager (default mencari folder sample_db di working dir)
sm = StorageManager(db_path="./sample_db")

# contoh: SELECT nim, nama FROM mahasiswa WHERE angkatan == 2022
dr = DataRetrieval(
    table='mahasiswa',
    columns=['nim', 'nama'],
    conditions=[Condition(column='angkatan', operator='==', value=2022)]
)

rows = sm.read_block(dr)
for r in rows:
    print(r['nim'], r['nama'])
```

Catatan penting untuk tim integrasi
----------------------------------
- Tipe return `read_block` adalah list of dict. Harap tangani kemungkinan list kosong bila tidak ada baris.
- `read_block` saat ini hanya membaca file JSON (sample_db/*.dat atau .json tergantung setup). Jika tim ingin format biner: gunakan `convert_to_binary.py` untuk mengkonversi dataset dan integrasikan `binary_storage.BinarySerializer` atau minta perubahan pada `StorageManager` untuk menambahkan flag `format='binary'`.
- Pastikan menangani exception ketika tabel tidak ditemukan.
- Untuk performa, referensi: `BinarySerializer` mendukung random-access O(1) per baris karena fixed-length rows — cocok bila Query Processor membutuhkan seek cepat.

Cara memanggil (API singkat)
---------------------------
Bagian ini menunjukkan pola pemanggilan praktis untuk integrasi dengan modul lain (mis. Query Processor). Pastikan `storage_manager` tersedia di PYTHONPATH atau jalankan dari root project.

1) Read (SELECT) — contoh lengkap

```python
from storage_manager.storage_manager import StorageManager
from storage_manager.models import DataRetrieval, Condition

# inisialisasi (default mencari folder sample_db di working dir)
sm = StorageManager(db_path='./sample_db')

# SELECT nim, nama FROM mahasiswa WHERE angkatan == 2022
dr = DataRetrieval(
    table='mahasiswa',
    columns=['nim', 'nama'],
    conditions=[Condition(column='angkatan', operator='==', value=2022)]
)

rows = sm.read_block(dr)
if not rows:
    print('Tidak ada hasil')
else:
    for r in rows:
        print(r)
```

2) Menambahkan baris (INSERT) — workaround sementara

Catatan: `StorageManager.write_block` belum diimplementasikan sepenuhnya. Sementara, Anda bisa menulis langsung ke file JSON tabel (sample_db) yang berupa list-of-rows.

```python
import json
from pathlib import Path

def append_row_json(db_path: str, table: str, new_row: dict):
    p = Path(db_path) / f"{table}.dat"
    data = []
    if p.exists():
        with p.open('r', encoding='utf-8') as fh:
            data = json.load(fh)
    data.append(new_row)
    with p.open('w', encoding='utf-8') as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)

# penggunaan
append_row_json('./sample_db', 'mahasiswa', {'nim': '2101', 'nama': 'Budi', 'angkatan': 2025})
```

3) Menghapus baris (DELETE) — workaround sementara

```python
def delete_rows_json(db_path: str, table: str, predicate):
    p = Path(db_path) / f"{table}.dat"
    if not p.exists():
        return 0
    with p.open('r', encoding='utf-8') as fh:
        data = json.load(fh)
    kept = [row for row in data if not predicate(row)]
    deleted_count = len(data) - len(kept)
    with p.open('w', encoding='utf-8') as fh:
        json.dump(kept, fh, ensure_ascii=False, indent=2)
    return deleted_count

# contoh: hapus mahasiswa angkatan 2020
# deleted = delete_rows_json('./sample_db', 'mahasiswa', lambda r: r.get('angkatan') == 2020)
```

4) Format biner (BinarySerializer)

Jika butuh akses acak cepat, gunakan `binary_storage.BinarySerializer` langsung atau konversikan dataset JSON ke biner menggunakan `convert_to_binary.py`.

```python
from storage_manager.binary_storage import BinarySerializer

# contoh schema (harus sesuai metadata)
schema = [
    ('nim', 'varchar', 16),
    ('nama', 'varchar', 64),
    ('angkatan', 'int', 4),
]
bs = BinarySerializer(schema)
rows = bs.binary_file_to_rows('./sample_db/mahasiswa.bin')
for r in rows[:10]:
    print(r)
```

5) Kontrak `DataWrite` / `DataDeletion` yang direkomendasikan

- DataWrite: {
    'table': str,
    'rows': List[dict],
    'mode': 'append' | 'replace'
}

- DataDeletion: {
    'table': str,
    'conditions': List[Condition]
}

Catatan: `write_block`/`delete_block` belum diimplementasikan sepenuhnya — gunakan workaround file JSON untuk integrasi cepat.

Langkah berikut & TODO
---------------------
- Integrasikan dukungan write/delete/index bila dibutuhkan oleh Query Processor (implementasi fungsi `write_block`, `delete_block`, `set_index`).
- Jika akan dipakai pada milestone akhir: ubah `StorageManager` agar bisa memilih antara JSON atau binary (flag di konstruktor) dan tambahkan konversi/penulisan biner yang aman.

Referensi & Skrip bantu
----------------------
- `setup_database.py`  — buat sample DB (JSON) untuk testing.
- `convert_to_binary.py` — konversi JSON → binary (gunakan `BinarySerializer`).
- `test_read_block.py` — test coverage untuk `read_block`.

Jika mau, saya bisa: (a) integrasikan binary langsung ke `StorageManager` (opsi) atau (b) implementasikan `write_block`/`delete_block` dasar — beri tahu mana yang prioritas.

Penulis: Tim Storage Manager (diworkspace)
Tanggal pembuatan: (otomatis oleh commit ini)

testing command : 

```
python -m storage_manager.test_storage_manager
```