import log
from pathlib import Path, PosixPath
from datetime import datetime
import os

class logFile:

        def __init__(self):

            base_dir = Path(__file__).resolve().parent
            log_path = base_dir / "log"

            # Ensure directory exists if not yet exist
            log_path.mkdir(parents=True,exist_ok=True)

            curr_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"logfile_{curr_time}.log"

            self.path: PosixPath = log_path / filename
            self.logFile_buffer: str = ""

        def __str__(self):
            return(
                f"filepath: {str(self.path)}\n"
                f"logFile_buffer: {self.logFile_buffer}"
            )
        
        def get_path(self):
            return self.path
        
        def make_new_file(self):
            base_dir = Path(__file__).resolve().parent
            log_path = base_dir / "log"

            curr_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"logfile_{curr_time}.log"
            self.path: PosixPath = log_path / filename

        def get_buffer(self):
            return self.logFile_buffer
        
        def load_file(self):
            with open(self.path) as f:
                self.logFile_buffer = f.readlines()
        
        def write_log(self, logItem: log):
            if(not self.path):
               self.make_new_file()

            with open(self.path, 'w') as f:
                f.write(str(logItem) + '\n')


