from datetime import datetime
import os
from enum import Enum

class LogType(Enum):
    WRITE = 0
    UPDATE = 1
    DELETE = 2

class Log:
    def __init__(self, id, log_type, data, timestamp):
        self.id = id
        self.log_type = log_type
        self.data = data
        self._timestamp = timestamp

    @property
    def timestamp(self):
        return self._timestamp

    @staticmethod
    def create_from_string(log_string: str):
        segments = log_string.split("|")
        if len(segments) != 4:
            raise ValueError("Invalid log string format")
        return Log(segments[0], segments[1], segments[2], segments[3])

    def to_string(self):
        return f"{self.id}|{self.log_type.name}|{self.data}|{self.timestamp}"

    def __repr__(self):
        return f"id={self.id}, log_type={self.log_type}, data={self.data}, timestamp={self.timestamp}"

class FileLogger:
    def __init__(self, log_directory, log_file):
        self.log_directory = log_directory
        self.log_file = log_file
        self.create_log_file()

    def create_log_file(self):
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)
        file_path = os.path.join(self.log_directory, self.log_file)
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                f.write("")  # Create an empty file

    def add_log(self, log):
        with open(os.path.join(self.log_directory, self.log_file), 'a') as f:
            f.write(log.to_string() + "\n")

    def read_logs(self):
        log_list = []
        with open(os.path.join(self.log_directory, self.log_file), 'r') as f:
            for line in f:
                # print('hhh')
                # print(line)
                log_list.append(Log.create_from_string(line))
        return log_list

    def get_last_log_id(self):
        last_log_id = None
        with open(os.path.join(self.log_directory, self.log_file), 'r') as f:
            for line in f:
                last_log = Log.create_from_string(line.strip())
                last_log_id = last_log.id
        if(last_log_id is None):
            return 0
        return last_log_id

# logger = FileLogger("logs", "logfile.log")

# # Creating a log instance
# log = Log("5", LogType(0), "Example log", datetime.now())

# # Adding the log to the file
# logger.add_log(log)

# # Reading logs
# logs = logger.read_logs()
# print("Logs:")
# for log in logs:
#     print(log)

# # Getting last log id
# last_log_id = logger.get_last_log_id()
# print("Last Log ID:", last_log_id)
