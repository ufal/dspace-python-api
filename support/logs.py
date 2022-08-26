"""
Logs. Everything seems self-explanatory to this author.
"""

from enum import Enum
from datetime import datetime
import atexit

all_logs = []
write_to_console = False
output_file_name = "logs.txt"
debug_output_file_name = "debug.log.txt"
date_file_name = "date.txt"


def set_up_logging():
    # set_write_to_console()
    turn_off_time()
    turn_off_output()
    turn_off_debug()
    pass


def set_debug_output_file_name(filename):
    global debug_output_file_name
    debug_output_file_name = filename


def turn_off_debug():
    set_debug_output_file_name(None)


def turn_off_time():
    set_time_file_name(None)


def turn_off_output():
    set_output_file_name(None)


def set_time_file_name(name):
    global date_file_name
    date_file_name = name


def set_output_file_name(name):
    global output_file_name
    output_file_name = name


def set_write_to_console(write=True):
    global write_to_console
    write_to_console = write


def turn_off_write_to_console():
    set_write_to_console(False)


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

    def __str__(self):
        return "[" + self.severity.name + "] " + self.message


def log(message, severity=Severity.INFO):
    log_object(LogMessage(str(message), severity))


def log_object(log_message: LogMessage):
    all_logs.append(log_message)
    if write_to_console:
        print(log_message)


def exit_handler():
    global output_file_name
    global date_file_name
    global all_logs
    if output_file_name is not None:
        of = open(output_file_name, "w+", encoding="utf-8")
        if all_logs:
            for one_log in all_logs:
                if Severity.DEBUG != one_log.severity:
                    print(one_log, file=of)
    if date_file_name is not None:
        df = open(date_file_name, "w+")
        print(datetime.now(), file=df)

    if debug_output_file_name is not None:
        dl = open(debug_output_file_name, "w+")
        if all_logs:
            for one_log in all_logs:
                print(one_log, file=dl)


atexit.register(exit_handler)
set_up_logging()
