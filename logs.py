from enum import Enum
import atexit

all_logs = []
write_to_console = False


def set_write_to_console():
    global write_to_console
    write_to_console = True


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
    print("logs:")
    global all_logs
    for one_log in all_logs:
        print(one_log.message)


atexit.register(exit_handler)
