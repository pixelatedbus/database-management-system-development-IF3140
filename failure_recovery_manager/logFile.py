from log import log
from pathlib import Path, PosixPath
from datetime import datetime
import os
from typing import List
import re
from ast import literal_eval
from fake_exec_result import ExecutionResult

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
        
        def get_logs(self):
            if(not  self.logFile_buffer):
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
            if(not self.path):
               self.make_new_file()
            
            try:
                with open(self.path, 'a') as f:
                    f.write(str(logItem) + '\n')
            
            except Exception as e:
                print(f"Error writing log to file: {e}")
        
        # TODO: DISCUSS WITH QUERY PROCESSOR ON THE CLARITY OF ExecutionResult
        def write_log_execRes(self, er: ExecutionResult):

            l = log(
                transaction_id=er.transaction_id,
                action=er.action,
                timestamp=er.timestamp,
                old_data=er.old_data,
                new_data=er.new_data,
                table_name=er.table_name
            )

            self.write_log(l)

            pass