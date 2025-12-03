from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

class Rows:
    def __init__(self, rows: list[dict]):
        self.rows = rows

class ConditionNode(ABC):
    @abstractmethod
    def evaluate(self, row: dict) -> bool:
        pass


class ComparisonNode(ConditionNode):
    def __init__(self, column: str, operator: str, operand: str):
        self.column = column
        self.operator = operator
        self.operand = operand
    
    def evaluate(self, row: dict) -> bool:
        if self.operator == "=":
            return row.get(self.column) == self.operand
        elif self.operator == "<>":
            return row.get(self.column) != self.operand
        elif self.operator == "<":
            return row.get(self.column) < self.operand
        elif self.operator == "<=":
            return row.get(self.column) <= self.operand
        elif self.operator == ">":
            return row.get(self.column) > self.operand
        elif self.operator == ">=":
            return row.get(self.column) >= self.operand
        else:
            raise ValueError(f"Unknown operator: {self.operator}")


class ANDNode(ConditionNode):
    def __init__(self, children: list[ConditionNode]):
        self.children = children
    
    def evaluate(self, row: dict) -> bool:
        return all(child.evaluate(row) for child in self.children)


class ORNode(ConditionNode):
    def __init__(self, children: list[ConditionNode]):
        self.children = children
    
    def evaluate(self, row: dict) -> bool:
        return any(child.evaluate(row) for child in self.children)


class NOTNode(ConditionNode):
    def __init__(self, child: ConditionNode):
        self.child = child
    
    def evaluate(self, row: dict) -> bool:
        return not self.child.evaluate(row)

@dataclass
class ColumnDefinition:
    """Definisi kolom untuk CREATE TABLE.

    Attributes:
        name: Nama kolom
        data_type: Tipe data ('INTEGER', 'FLOAT', 'CHAR', 'VARCHAR')
        size: Ukuran untuk CHAR(n) atau VARCHAR(n). None untuk INTEGER/FLOAT
        is_primary_key: Apakah kolom ini PRIMARY KEY
        is_nullable: Apakah kolom ini boleh NULL (default: True)
        default_value: Nilai default jika tidak diisi
    """
    name: str
    data_type: str  # 'INTEGER', 'FLOAT', 'CHAR', 'VARCHAR'
    size: Optional[int] = None  # untuk CHAR(n), VARCHAR(n)
    is_primary_key: bool = False
    is_nullable: bool = True
    default_value: Any = None

    def __post_init__(self):
        """Validasi setelah inisialisasi."""
        # Normalisasi data_type ke uppercase
        self.data_type = self.data_type.upper()

        # Validasi tipe data
        valid_types = ['INTEGER', 'FLOAT', 'CHAR', 'VARCHAR']
        if self.data_type not in valid_types:
            raise ValueError(f"Invalid data type: {self.data_type}. Must be one of {valid_types}")

        # CHAR dan VARCHAR harus punya size
        if self.data_type in ['CHAR', 'VARCHAR']:
            if self.size is None or self.size <= 0:
                raise ValueError(f"{self.data_type} must have size > 0")

        # PRIMARY KEY tidak boleh nullable
        if self.is_primary_key:
            self.is_nullable = False


@dataclass
class ForeignKey:
    """Definisi FOREIGN KEY constraint.

    Attributes:
        column: Nama kolom di tabel ini
        references_table: Nama tabel yang direferensi
        references_column: Nama kolom di tabel yang direferensi
        on_delete: Aksi saat DELETE ('CASCADE', 'SET NULL', 'RESTRICT')
        on_update: Aksi saat UPDATE ('CASCADE', 'SET NULL', 'RESTRICT')
    """
    column: str
    references_table: str
    references_column: str
    on_delete: str = "RESTRICT"  # CASCADE, SET NULL, RESTRICT
    on_update: str = "RESTRICT"

    def __post_init__(self):
        """Validasi setelah inisialisasi."""
        self.on_delete = self.on_delete.upper()
        self.on_update = self.on_update.upper()

        valid_actions = ['CASCADE', 'SET NULL', 'RESTRICT']
        if self.on_delete not in valid_actions:
            raise ValueError(f"Invalid on_delete: {self.on_delete}. Must be one of {valid_actions}")
        if self.on_update not in valid_actions:
            raise ValueError(f"Invalid on_update: {self.on_update}. Must be one of {valid_actions}")


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
    
    # Alias for backward compatibility
    @property
    def operator(self) -> str:
        """Alias for operation (legacy compatibility)."""
        return self.operation
    
    def evaluate(self, row: dict) -> bool:
        """Evaluate condition against a row (legacy compatibility)."""
        if self.operation == "=":
            return row.get(self.column) == self.operand
        elif self.operation == "<>":
            return row.get(self.column) != self.operand
        elif self.operation == "<":
            return row.get(self.column) < self.operand
        elif self.operation == "<=":
            return row.get(self.column) <= self.operand
        elif self.operation == ">":
            return row.get(self.column) > self.operand
        elif self.operation == ">=":
            return row.get(self.column) >= self.operand
        else:
            raise ValueError(f"Unknown operator: {self.operation}")
    
    def __repr__(self):
        return f"Condition({self.column} {self.operation} {self.operand})"


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
class DataUpdate:
    """Parameter untuk operasi update_by_old_new_data untuk FRM.
    
    Simple update based on old/new data matching - no conditions needed!
    
    Attributes:
        table: Nama tabel yang akan diupdate
        old_data: List of rows yang mau diganti (matching criteria)
        new_data: List of rows pengganti (harus sama panjangnya dengan old_data)
    
    Example:
        DataUpdate(
            table="users",
            old_data=[{"id": 1, "name": "Alice", "status": "inactive"}],
            new_data=[{"id": 1, "name": "Alice", "status": "active"}]
        )
    """
    table: str
    old_data: List[Dict[str, Any]] = field(default_factory=list)
    new_data: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class Statistic:
    """Statistik tabel untuk query optimization.
    
    Atribut:
        n_r: Jumlah tuple dalam relasi r
        b_r: Jumlah blok yang berisi tuple dari r
        l_r: Ukuran tuple dari r (dalam bytes)
        f_r: Blocking factor dari r (jumlah tuple yang muat dalam satu blok)
        V_a_r: Dictionary mapping kolom -> jumlah nilai distinct di kolom tersebut
        indexes: Dictionary mapping kolom -> info index (type dan height untuk btree)
                 Format: {"column_name": {"type": "hash"|"btree", "height": int (for btree only)}}
    
    Rumus:
        b_r = ceil(n_r / f_r)  jika tuple disimpan bersama secara fisik dalam satu file
    """
    n_r: int
    b_r: int
    l_r: int
    f_r: int
    V_a_r: Dict[str, int] = field(default_factory=dict)
    indexes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
