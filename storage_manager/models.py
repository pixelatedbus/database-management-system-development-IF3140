"""Model dataclass untuk Storage Manager."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class Condition:
    """Merepresentasikan satu kondisi untuk filtering data.
    
    Attributes:
        column: Nama kolom yang digunakan dalam kondisi
        operation: Operator perbandingan ('=', '<>', '<', '<=', '>', '>=')
        operand: Nilai yang dibandingkan (int | str)
    """
    column: str
    operation: str  # '=', '<>', '<', '<=', '>', '>='
    operand: Any  # int | str


@dataclass
class DataRetrieval:
    """Parameter untuk operasi read_block.
    
    Attributes:
        table: Nama tabel yang akan dibaca
        column: Daftar kolom yang ingin diambil (proyeksi). Jika kosong, ambil semua kolom
        conditions: Daftar kondisi untuk filtering (AND logic)
    """
    table: str
    column: List[str] = field(default_factory=list)
    conditions: List[Condition] = field(default_factory=list)


@dataclass
class DataWrite:
    """Parameter untuk operasi write_block (insert/update).
    
    Attributes:
        table: Nama tabel yang akan diisi/diupdate
        column: Daftar kolom yang akan diisi/diupdate
        conditions: Daftar kondisi untuk UPDATE (jika kosong = INSERT)
        new_value: Daftar nilai baru (harus sesuai urutan column)
    """
    table: str
    column: List[str] = field(default_factory=list)
    conditions: List[Condition] = field(default_factory=list)
    new_value: List[Any] = field(default_factory=list)


@dataclass
class DataDeletion:
    """Parameter untuk operasi delete_block.
    
    Attributes:
        table: Nama tabel yang akan dihapus
        conditions: Daftar kondisi untuk menentukan row yang dihapus (AND logic)
    """
    table: str
    conditions: List[Condition] = field(default_factory=list)


@dataclass
class Statistic:
    """Statistik tabel untuk query optimization.
    
    Atribut:
        n_r: Jumlah tuple dalam relasi r
        b_r: Jumlah blok yang berisi tuple dari r
        l_r: Ukuran tuple dari r (dalam bytes)
        f_r: Blocking factor dari r (jumlah tuple yang muat dalam satu blok)
        V_a_r: Dictionary mapping kolom -> jumlah nilai distinct di kolom tersebut
    
    Rumus:
        b_r = ceil(n_r / f_r)  jika tuple disimpan bersama secara fisik dalam satu file
    """
    n_r: int
    b_r: int
    l_r: int
    f_r: int
    V_a_r: Dict[str, int] = field(default_factory=dict)
