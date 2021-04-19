import sys

INFO = "\033[96m"
WARN = "\033[93m"
ERROR = "\033[91m"
RESET = "\033[0m"
BOLD = "\033[1m"
UNDERLINE = "\033[4m"

if sys.argv[1] == "INFO":
    COLOR = INFO
    TAG = "[INFO]"
elif sys.argv[1] == "WARN":
    COLOR = WARN
    TAG = "[WARN]"
elif sys.argv[1] == "ERROR":
    COLOR = ERROR
    TAG = "[ERROR]"

print(BOLD + COLOR + TAG + sys.argv[2] + RESET)
