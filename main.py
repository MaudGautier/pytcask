import os
from collections import namedtuple
from time import time


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


class File:
    Offset = int
    TIMESTAMP_LENGTH = 10  # Assuming we don't use this DB later than year 2286 😁
    KEY_VALUE_PAIR_SEPARATOR = "\n"

    def __init__(self, path: str, mode: str):
        self.file = open(path, mode=mode)
        self.path = path

    @staticmethod
    def read(path: str, start: int, end: int):
        with open(path, "r") as file:
            file.seek(start)
            value = file.read(end - start)
            return value


class ActiveFile(File):
    def __init__(self, path: str):
        super().__init__(path=path, mode="w")

    def _append(self, item: DatabaseItem) -> File.Offset:
        self.file.write(item.metadata)
        self.file.write(item.key)
        value_position_offset = self._current_offset
        self.file.write(item.value)
        self.file.write(self.KEY_VALUE_PAIR_SEPARATOR)
        self.file.flush()
        return value_position_offset

    @property
    def _current_offset(self) -> File.Offset:
        return self.file.tell()

    @property
    def size(self) -> File.Offset:
        return self._current_offset

    def append(self, item: DatabaseItem) -> File.Offset:
        return self._append(item=item)

    def close(self) -> None:
        self.file.close()

    def convert_to_immutable(self, new_path: str) -> None:
        self.file.close()
        os.rename(src=self.path, dst=new_path)


class ImmutableFile(File):
    def __init__(self, path: str):
        super().__init__(path=path, mode="r")


class KeyDir:
    KeyDirEntry = namedtuple(
        "KeyDirEntry", ["file_path", "value_position", "value_size"]
    )

    def __init__(self):
        self.content = {}

    def update(
        self,
        key: DatabaseItem.Key,
        file_path: str,
        value_position: File.Offset,
        value_size: int,
    ) -> None:
        self.content[key] = self.KeyDirEntry(
            file_path=file_path, value_position=value_position, value_size=value_size
        )

    def update_file_path(self, previous_path: str, new_path: str) -> None:
        for key, key_dir_entry in self.content.items():
            if key_dir_entry.file_path == previous_path:
                self.update(
                    key=key,
                    file_path=new_path,
                    value_position=key_dir_entry.value_position,
                    value_size=key_dir_entry.value_size,
                )

    def get(self, key: DatabaseItem.Key):
        return self.content[key] if key in self.content else None


class Database:
    DEFAULT_DIRECTORY = "./datafiles/"
    # ACTIVE_FILE_THRESHOLD is an offset number (= number of characters since the start)
    ACTIVE_FILE_THRESHOLD = 150

    def __init__(self, directory=DEFAULT_DIRECTORY):
        self.directory = directory
        self.active_file_path = directory + "active.txt"
        self.active_file = ActiveFile(path=self.active_file_path)
        self.key_dir = KeyDir()

    def _generate_new_active_file(self):
        immutable_file_path = f"{self.directory}{int(time())}.txt"
        self.active_file.convert_to_immutable(new_path=immutable_file_path)
        self.key_dir.update_file_path(
            previous_path=self.active_file_path, new_path=immutable_file_path
        )
        self.active_file = ActiveFile(self.active_file_path)

    def _append_to_active_file(self, item: DatabaseItem) -> File.Offset:
        new_line_size = len(item.metadata) + item.key_size + item.value_size
        expected_file_size = self.active_file.size + new_line_size
        is_active_file_too_big = expected_file_size > Database.ACTIVE_FILE_THRESHOLD

        if is_active_file_too_big:
            self._generate_new_active_file()

        value_position_offset = self.active_file.append(item)
        return value_position_offset

    # ~~~~~~~~~~~~~~~~~~~
    # ~~~ API
    # ~~~~~~~~~~~~~~~~~~~

    def append(self, key: DatabaseItem.Key, value: DatabaseItem.Value):
        """When appending, we need to have an operation that atomically performs the following two things:
        1. Append the key-value pair to the currently active file
        2. Add the key to the keyDir in-memory structure.
        """
        # TODO: think about a way to encapsulate these two in an atomic operation so that either both or none is performed!
        item = DatabaseItem(key=key, value=value)
        active_file_value_position_offset = self._append_to_active_file(item)
        self.key_dir.update(
            key=key,
            file_path=self.active_file_path,
            value_position=active_file_value_position_offset,
            value_size=item.value_size,
        )

    def get(self, key: DatabaseItem.Key) -> DatabaseItem.Value or None:
        """Returns the value for the key searched.
        If there is no such key in the database, returns None.
        """
        key_dir_entry = self.key_dir.get(key)
        if not key_dir_entry:
            return None

        return File.read(
            path=key_dir_entry.file_path,
            start=key_dir_entry.value_position,
            end=key_dir_entry.value_position + key_dir_entry.value_size,
        )


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

    missing_key = "i_dont_exist"
    print(f"Here is the value of {missing_key}", database.get(missing_key))
