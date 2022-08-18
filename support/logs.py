from enum import Enum
from datetime import datetime
import atexit



all_logs = []
write_to_console = False
output_file_name = "logs.txt"
date_file_name = "date.txt"

def set_write_to_console():
    global write_to_console
    write_to_console = True

def set_output_file(name):
    global output_file_name
    output_file_name = name

class Severity(Enum):
    TRACE = 1
    DEBUG = 2
    INFO = 3
    WARN = 4
    ERROR = 5


class LogMessage:
    def __init__(self, msg, severity):
        self.message = msg
        self.severity = severity


def log(message, severity=Severity.INFO):
    log_object(LogMessage(str(message), severity))


def log_object(log_message: LogMessage):
    all_logs.append(log_message)
    if write_to_console:
        print(log_message.message)


def exit_handler():
    global output_file_name
    global date_file_name
    global all_logs
    if output_file_name is not None:
        of = open(output_file_name, "w+")
        if all_logs:
            print("logs", file=of)
        for one_log in all_logs:
            print(one_log.message, file=of)
    df = open(date_file_name, "w+")
    print(datetime.now(), file=df)


atexit.register(exit_handler)
