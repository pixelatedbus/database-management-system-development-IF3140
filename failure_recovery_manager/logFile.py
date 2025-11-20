from log import log
from pathlib import Path, PosixPath
from datetime import datetime
import os
from typing import List, Optional
import re
from ast import literal_eval
import hashlib

class logFile:

        def __init__(self):

            base_dir = Path(__file__).resolve().parent
            log_path = base_dir / "log"

            # Ensure directory exists if not yet exist
            log_path.mkdir(parents=True,exist_ok=True)

            curr_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename_1 = f"logfile_{curr_time}_1.log"
            filename_2 = f"logfile_{curr_time}_2.log"

            self.paths: PosixPath = [log_path / filename_1, log_path / filename_2]
            self.logFile_buffer: str = ""

        def __str__(self):
            return(
                f"filepaths: {str(self.paths)}\n"
                f"logFile_buffer: {self.logFile_buffer}"
            )
        
        def get_path(self):
            return self.paths
        
        def make_new_file(self):
            base_dir = Path(__file__).resolve().parent
            log_path = base_dir / "log"

            curr_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename_1 = f"logfile_{curr_time}_1.log"
            filename_2 = f"logfile_{curr_time}_2.log"

            self.paths: PosixPath = [log_path / filename_1, log_path / filename_2]

        def get_buffer(self):
            return self.logFile_buffer
        
        def verify_checksum(self, raw_line: str) -> Optional[str]:
            line = raw_line.rstrip("\n")
            if line == "":
                return None
            
            try:
                checksum, data = line.split("|", 1)
            except ValueError:
                return None

            new_checksum = hashlib.sha256(data.encode('utf-8')).hexdigest()
            if checksum != new_checksum:
                return None
            return data

        def load_file(self):
            self.logFile_buffer = []
            logFile_buffers = []
            for i in range(len(self.paths)):
                try:
                    with open(self.paths[i]) as f:
                        logFile_buffers.append(f.readlines())
                except FileNotFoundError:
                    continue

            if not logFile_buffers:
                raise FileNotFoundError("No log files found")

            max_len = max(map(len, logFile_buffers))

            for i in range(max_len):
                valid = []

                for b in logFile_buffers:
                    if i >= len(b):
                        continue

                    raw_line = b[i]
                    data = self.verify_checksum(raw_line)
                    if data is not None:
                        valid.append(data)

                # append to buffer
                self.logFile_buffer.append(valid[0] + "\n")  # there's a downside of using this... subject to change
                print(self.logFile_buffer)

                # TODO: fix broken log files
        
        def get_logs(self):
            # enforcing loading of log file (might change idk)
            self.load_file()
            
            l_list: List[log] = []

            parse_pattern = re.compile(
                r"transaction_id:(?P<transaction_id>\d+),\s*"
                r"action:(?P<action>\d+),\s*"
                r"timestamp:(?P<timestamp>[\d\s\.:_-]+),\s*"
                r"old_data:(?P<old_data>\{[^}]*\}),\s*"
                r"new_data:(?P<new_data>\{[^}]*\}),\s*"
                r"table_name:(?P<table_name>.*?)[\s,]*\n?$"
            )   
            
            for log_str in self.logFile_buffer:

                match = parse_pattern.match(log_str)

                if not match:
                    raise ValueError(f"Log string format not recognized: {log_str}")

                data = match.groupdict()

                # convert timestamp into datetime
                try:
                    time_format = "%Y-%m-%d_%H-%M-%S"
                    data['timestamp'] = datetime.strptime(data['timestamp'], time_format)
                except ValueError as e:
                    print(f"Error evaluating timestamp in log: {log_str}, Error: {e}")
                    print(f"Setting timestamp to current time")

                    # IMPORTANT, THIS NEEDS TO MATCH HOW CLASS LOG WORKS
                    data['timestamp'] = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


                # evaluate old/new_data into dictionary
                try:
                    data['old_data'] = literal_eval(data['old_data'])
                    data['new_data'] = literal_eval(data['new_data'])
                except (ValueError, SyntaxError) as e:
                    print(f"Error evaluating dictionary data in log: {log_str}. Error: {e}")
                    print(f"Setting dictionary data empty")
                    data['old_data'] = {}
                    data['new_data'] = {}
                
                l_list.append(log(
                    transaction_id=data['transaction_id'],
                    action=data['action'],
                    timestamp=data['timestamp'],
                    new_data=data['new_data'],
                    old_data=data['old_data'],
                    table_name=data['table_name']
                ))

            return l_list
        
        def write_log(self, logItem: log):
            if(not self.paths or not self.paths[0]):
               self.make_new_file()

            data = str(logItem)
            checksum = hashlib.sha256(data.encode('utf-8')).hexdigest()

            for path in self.paths:
                with open(path, 'a') as f:
                    f.write(f"{checksum}|{data}\n")


