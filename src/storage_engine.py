from time import time

from src.item import Item
from src.io_handling import ActiveFile, File
from src.key_dir import KeyDir


class StorageEngine:
    DEFAULT_DIRECTORY = "./datafiles/default/"
    DEFAULT_MAX_FILE_SIZE = 150

    def __init__(
        self, directory=DEFAULT_DIRECTORY, max_file_size=DEFAULT_MAX_FILE_SIZE
    ):
        self.directory = directory
        self.active_file_path = directory + "active.txt"
        self.active_file = ActiveFile(path=self.active_file_path)
        self.key_dir = KeyDir()
        self.max_file_size = max_file_size

    def _generate_new_active_file(self):
        # Using time in nanoseconds to avoid filename collisions
        timestamp_in_ns = int(time() * 1000000)
        immutable_file_path = f"{self.directory}{timestamp_in_ns}.txt"
        self.active_file.convert_to_immutable(new_path=immutable_file_path)
        self.key_dir.update_file_path(
            previous_path=self.active_file_path, new_path=immutable_file_path
        )
        self.active_file = ActiveFile(self.active_file_path)

    def _append_to_active_file(self, item: Item) -> File.Offset:
        new_line_size = len(item.metadata) + item.key_size + item.value_size
        expected_file_size = self.active_file.size + new_line_size
        is_active_file_too_big = expected_file_size > self.max_file_size

        if is_active_file_too_big:
            self._generate_new_active_file()

        value_position_offset = self.active_file.append(item)
        return value_position_offset

    # ~~~~~~~~~~~~~~~~~~~
    # ~~~ API
    # ~~~~~~~~~~~~~~~~~~~

    def append(self, key: Item.Key, value: Item.Value):
        """When appending, we need to have an operation that atomically performs the following two things:
        1. Append the key-value pair to the currently active file
        2. Add the key to the keyDir in-memory structure.
        """
        item = Item(key=key, value=value)
        active_file_value_position_offset = self._append_to_active_file(item)
        self.key_dir.update(
            key=key,
            file_path=self.active_file_path,
            value_position=active_file_value_position_offset,
            value_size=item.value_size,
        )

    def get(self, key: Item.Key) -> Item.Value or None:
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
