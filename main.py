import os
from collections import namedtuple
from time import time
from typing import TextIO


# ~~~~~~~~~~~~~~~~~~~
# ~~~ Constants
# ~~~~~~~~~~~~~~~~~~~
class DatabaseItem:
    Key = str
    Value = str

    def __init__(self, key: Key, value: Value):
        self.key = key
        self.value = value

    @property
    def crc(self) -> str:
        return ""

    @property
    def key_size(self) -> int:
        return len(self.key)

    @property
    def value_size(self) -> int:
        return len(self.value)

    @property
    def metadata(self) -> str:
        return f"{self.crc}{int(time())}{self.key_size}{self.value_size}"

class Database:
    Offset = int
    DEFAULT_DIRECTORY = "./datafiles/"
    TIMESTAMP_LENGTH = 10  # Assuming we don't use this DB later than year 2286 😁
    KEY_VALUE_PAIR_SEPARATOR = "\n"
    ACTIVE_FILE_THRESHOLD = 150  # This is an offset number (= number of characters since the start)
    KeyDirEntry = namedtuple('KeyDirEntry', ['file_path', 'value_position', 'value_size'])

    def __init__(self, directory=DEFAULT_DIRECTORY):
        self.directory = directory
        self.active_file_path = directory + "active.txt"
        self.active_file = open(self.active_file_path, "w")
        self.key_dir = {}

    @property
    def _current_offset(self) -> Offset:
        return self.active_file.tell()

    def _write_to_active_file(self, item: DatabaseItem) -> Offset:
        self.active_file.write(item.metadata)
        self.active_file.write(item.key)
        value_position_offset = self._current_offset
        self.active_file.write(item.value)
        self.active_file.write(Database.KEY_VALUE_PAIR_SEPARATOR)
        self.active_file.flush()
        return value_position_offset

    def _append_to_active_file(self, item: DatabaseItem) -> Offset:
        key_value_metadata_line = item.metadata
        active_file_size = self._current_offset

        if active_file_size + len(
                key_value_metadata_line) + item.key_size + item.value_size > Database.ACTIVE_FILE_THRESHOLD:
            self.active_file.close()
            # rename it
            immutable_file_path = f"{self.directory}{int(time())}.txt"
            os.rename(src=self.active_file_path, dst=immutable_file_path)

            # Update the in-memory KEY_DIR
            for key, key_dir_entry in self.key_dir.items():
                if key_dir_entry.file_path == self.active_file_path:
                    self.key_dir[key] = Database.KeyDirEntry(file_path=immutable_file_path,
                                                             value_position=key_dir_entry.value_position,
                                                             value_size=key_dir_entry.value_size)

            # open the new active one
            self.active_file = open(self.active_file_path, "w")

        value_position_offset = self._write_to_active_file(item)
        return value_position_offset

    def _update_keydir(self, key: DatabaseItem.Key, file_path: str, value_position: Offset, value_size: int) -> None:
        self.key_dir[key] = Database.KeyDirEntry(file_path=file_path, value_position=value_position, value_size=value_size)

    # ~~~~~~~~~~~~~~~~~~~
    # ~~~ API
    # ~~~~~~~~~~~~~~~~~~~

    def append(self, key: DatabaseItem.Key, value: DatabaseItem.Value):
        """ When appending, we need to have an operation that atomically performs the following two things:
        1. Append the key-value pair to the currently active file
        2. Add the key to the keyDir in-memory structure.
        """
        # TODO: think about a way to encapsulate these two in an atomic operation so that either both or none is performed!
        item = DatabaseItem(key=key, value=value)
        active_file_value_position_offset = self._append_to_active_file(item)
        value_size = item.value_size
        self._update_keydir(key=key,
                            file_path=self.active_file_path,
                            value_position=active_file_value_position_offset,
                            value_size=value_size)

    def get(self, key: DatabaseItem.Key) -> DatabaseItem.Value or None:
        """ Returns the value for the key searched. If there is no such key in the database, return None.
        """
        if key not in self.key_dir:
            return None

        key_dir_entry = self.key_dir[key]
        with open(key_dir_entry.file_path, "r") as file_reader:
            file_reader.seek(key_dir_entry.value_position)
            value = file_reader.read(key_dir_entry.value_size)
            return value


# ~~~~~~~~~~~~~~~~~~~
# ~~~ Temporary tests
# ~~~~~~~~~~~~~~~~~~~

if __name__ == "__main__":
    print("Starting !")
    database = Database()
    database.append(key="key1", value="value1")
    database.append(key="key2", value="value2")
    database.append(key="key3", value="value3")
    database.append(key="key4", value="value4")
    searched_keys = ["key1", "key2", "key3", "key4"]
    for searched_key in searched_keys:
        print(f"Here is the value of {searched_key}", database.get(searched_key))
    database.append(key="key1", value="another_value1")
    database.append(key="key2", value="another_value2")
    database.append(key="key3", value="another_value3")
    database.append(key="key1", value="yet_another_value1")
    searched_keys = ["key1", "key2", "key3", "key4"]
    for searched_key in searched_keys:
        print(f"Here is the value of {searched_key}", database.get(searched_key))
