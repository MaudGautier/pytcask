import os
from time import time
from typing import TextIO

# ~~~~~~~~~~~~~~~~~~~
# ~~~ Constants
# ~~~~~~~~~~~~~~~~~~~

DIRECTORY = "./datafiles/"
ACTIVE_FILE_PATH = DIRECTORY + "active.txt"
TIMESTAMP_LENGTH = 10  # Assuming we don't use this DB later than year 2286 😁
KEY_DIR = {}
KEY_VALUE_PAIR_SEPARATOR = "\n"
ACTIVE_FILE_THRESHOLD = 150  # This is an offset number (= number of characters since the start)

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


def write_to_active_file(metadata: str, key: Key, value: Value) -> Offset:
    ACTIVE_FILE.write(metadata)
    ACTIVE_FILE.write(key)
    value_position_offset = get_current_offset(file=ACTIVE_FILE)
    ACTIVE_FILE.write(value)
    ACTIVE_FILE.write(KEY_VALUE_PAIR_SEPARATOR)
    ACTIVE_FILE.flush()
    return value_position_offset


def append_to_active_file(key: Key, value: Value) -> Offset:
    key_value_metadata_line = define_key_value_metadata(key=key, value=value)
    active_file_size = get_current_offset(file=ACTIVE_FILE)

    if active_file_size + len(key_value_metadata_line) + len(key) + len(value) > ACTIVE_FILE_THRESHOLD:
        ACTIVE_FILE.close()
        # rename it
        immutable_file_path = f"{DIRECTORY}{int(time())}.txt"
        os.rename(src=ACTIVE_FILE_PATH, dst=immutable_file_path)

        # Update the in-memory KEY_DIR
        for key, key_dir_value in KEY_DIR.items():
            file_path, value_size, value_position = key_dir_value
            if file_path == ACTIVE_FILE_PATH:
                KEY_DIR[key] = (immutable_file_path, value_size, value_position)

        # open the new active one
        globals()['ACTIVE_FILE'] = open(ACTIVE_FILE_PATH, "w")

    value_position_offset = write_to_active_file(metadata=key_value_metadata_line, key=key, value=value)
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
    update_keydir(key=key,
                  file_path=ACTIVE_FILE_PATH,
                  value_position=active_file_value_position_offset,
                  value_size=value_size)


def get(key: Key) -> Value or None:
    """ Returns the value for the key searched. If there is no such key in the database, return None.
    """
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
    searched_keys = ["key1", "key2", "key3", "key4"]
    for searched_key in searched_keys:
        print(f"Here is the value of {searched_key}", get(searched_key))
    append(key="key1", value="another_value1")
    append(key="key2", value="another_value2")
    append(key="key3", value="another_value3")
    append(key="key1", value="yet_another_value1")
    searched_keys = ["key1", "key2", "key3", "key4"]
    for searched_key in searched_keys:
        print(f"Here is the value of {searched_key}", get(searched_key))
