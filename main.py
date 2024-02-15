from time import time
from typing import TextIO

# ~~~~~~~~~~~~~~~~~~~
# ~~~ Constants
# ~~~~~~~~~~~~~~~~~~~

DIRECTORY = "./datafiles/"
ACTIVE_FILE_PATH = DIRECTORY + "active.txt"
TIMESTAMP_LENGTH = 10  # Assuming we don't use this DB later than year 2286 😁

# ~~~~~~~~~~~~~~~~~~~
# ~~~ Types
# ~~~~~~~~~~~~~~~~~~~

Key = str
Value = str
Offset = int

# ~~~~~~~~~~~~~~~~~~~
# ~~~ Server
# ~~~~~~~~~~~~~~~~~~~

ACTIVE_FILE = open(ACTIVE_FILE_PATH, "w")


def compute_crc(key: Key, value: Value):
    return ""


def compute_size(string: Key or Value):
    return len(string)


def define_key_value_line(key: Key, value: Value):
    crc = compute_crc(key=key, value=value)
    timestamp = str(int(time()))
    key_size = str(compute_size(key))
    value_size = str(compute_size(value))

    return crc + timestamp + key_size + value_size + key + value + "\n"


def get_current_offset(file: TextIO) -> Offset:
    return file.tell()


def append_to_active_file(key: Key, value: Value) -> Offset:
    key_value_line = define_key_value_line(key=key, value=value)
    offset = get_current_offset(file=ACTIVE_FILE)
    ACTIVE_FILE.write(key_value_line)
    return offset


# ~~~~~~~~~~~~~~~~~~~
# ~~~ API
# ~~~~~~~~~~~~~~~~~~~

def append(key: Key, value: Value):
    """ When appending, we need to have an operation that atomically performs the following two things:
    1. Append the key-value pair to the currently active file
    2. Add the key to the keyDir in-memory structure.
    """
    # TODO: think about a way to encapsulate these two in an atomic operation so that either both or none is performed!
    active_file_offset = append_to_active_file(key=key, value=value)
    update_keydir(key=key, file=ACTIVE_FILE, offset=active_file_offset)


# ~~~~~~~~~~~~~~~~~~~
# ~~~ Temporary tests
# ~~~~~~~~~~~~~~~~~~~

if __name__ == "__main__":
    print("Starting !")
    append(key="key1", value="value1")
