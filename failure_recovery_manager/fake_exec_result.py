from datetime import datetime
from concurrency_control_manager.src.row import Row
from typing import List

class ExecutionResult:

	def __init__(self):
		self.transaction_id:int = 0
		self.timestamp: datetime = datetime.now()
		self.message: str = ""
		self.query: str = ""
		# OK INI NNTI HARUS MINTA DARI QUERY PROCESSER BUAT ADA NEW DATA, OLD DATA, dan Action
		# and maybe table name that'd be nice :D
		self.old_data: List[Row] | int = []
		self.new_data: List[Row] | int = []
		self.action: str = ""
		self.table_name: str = ""