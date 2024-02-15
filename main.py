from time import time
from typing import TextIO

# ~~~~~~~~~~~~~~~~~~~
# ~~~ Constants
# ~~~~~~~~~~~~~~~~~~~

DIRECTORY = "./datafiles/"
ACTIVE_FILE_PATH = DIRECTORY + "active.txt"
TIMESTAMP_LENGTH = 10  # Assuming we don't use this DB later than year 2286 😁
KEY_DIR = {}

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


def define_key_value_metadata(key: Key, value: Value):
    crc = compute_crc(key=key, value=value)
    timestamp = str(int(time()))
    key_size = str(compute_size(key))
    value_size = str(compute_size(value))

    return crc + timestamp + key_size + value_size


def get_current_offset(file: TextIO) -> Offset:
    return file.tell()


def append_to_active_file(key: Key, value: Value) -> Offset:
    key_value_metadata_line = define_key_value_metadata(key=key, value=value)
    ACTIVE_FILE.write(key_value_metadata_line)
    ACTIVE_FILE.write(key)
    value_position_offset = get_current_offset(file=ACTIVE_FILE)
    ACTIVE_FILE.write(value)
    return value_position_offset


def update_keydir(key: Key, file_path: str, value_position: Offset, value_size: int) -> None:
    KEY_DIR[key] = (file_path, value_size, value_position)


# ~~~~~~~~~~~~~~~~~~~
# ~~~ API
# ~~~~~~~~~~~~~~~~~~~

def append(key: Key, value: Value):
    """ When appending, we need to have an operation that atomically performs the following two things:
    1. Append the key-value pair to the currently active file
    2. Add the key to the keyDir in-memory structure.
    """
    # TODO: think about a way to encapsulate these two in an atomic operation so that either both or none is performed!
    active_file_value_position_offset = append_to_active_file(key=key, value=value)
    value_size = compute_size(value)
    update_keydir(key=key, file_path=ACTIVE_FILE_PATH, value_position=active_file_value_position_offset,
                  value_size=value_size)


def get(key: Key) -> Value or None:
    if key not in KEY_DIR:
        return None

    (file_path, value_size, value_position_offset) = KEY_DIR[key]
    with open(file_path, "r") as file_reader:
        file_reader.seek(value_position_offset)
        value = file_reader.read(value_size)
        return value


# ~~~~~~~~~~~~~~~~~~~
# ~~~ Temporary tests
# ~~~~~~~~~~~~~~~~~~~

if __name__ == "__main__":
    print("Starting !")
    append(key="key1", value="value1")
    append(key="key2", value="value2")
    append(key="key3", value="value3")
    append(key="key4", value="value4")
    print(get("key3"))
