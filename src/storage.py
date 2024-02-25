import os
from time import time

from src.io_handling import (
    ActiveFile,
    StoredItem,
    File,
    HintFile,
    ReadableFile,
    FileType,
)
from src.item import Item
from src.key_dir import KeyDir


class Storage:
    def __init__(self, directory: str, max_file_size: int):
        self.directory = directory
        self.active_file = ActiveFile(path=f"{self.directory}/active.data")
        self.max_file_size = max_file_size
        self.key_dir = KeyDir()
        self.rebuild_index()

    def _generate_new_active_file(self) -> None:
        # Using time in nanoseconds to avoid filename collisions
        timestamp_in_ns = int(time() * 1_000_000)
        immutable_file_path = f"{self.directory}/{timestamp_in_ns}.data"
        self.active_file.convert_to_immutable(new_path=immutable_file_path)
        self.key_dir.update_file_path(
            previous_path=self.active_file.path, new_path=immutable_file_path
        )
        self.active_file = ActiveFile(self.active_file.path)

    def _append_to_active_file(self, stored_item: StoredItem) -> File.Offset:
        new_line_size = stored_item.size
        expected_file_size = self.active_file.size + new_line_size
        is_active_file_too_big = expected_file_size > self.max_file_size

        if is_active_file_too_big:
            self._generate_new_active_file()

        value_position_offset = self.active_file.append(stored_item=stored_item)
        return value_position_offset

    def _get_index_rebuild_files(self) -> tuple[list[ReadableFile], list[HintFile]]:
        hint_files = []
        unmerged_data_files = []
        for filename in os.listdir(self.directory):
            file_path = f"{self.directory}/{filename}"
            file = ReadableFile(path=file_path)
            if file.type == FileType.HINT:
                hint_files.append(HintFile(path=file_path, read_only=True))
            if file.type == FileType.UNMERGED_DATA:
                unmerged_data_files.append(file)
        return unmerged_data_files, hint_files

    # ~~~~~~~~~~~~~~~~~~~
    # ~~~ API
    # ~~~~~~~~~~~~~~~~~~~

    def append(self, key: Item.Key, value: Item.Value) -> None:
        """When appending, we need to have an operation that atomically performs the following two things:
        1. Append the key-value pair to the currently active file
        2. Add the key to the keyDir in-memory structure.
        """
        item = Item(key=key, value=value)
        stored_item = StoredItem.from_item(item=item)
        active_file_value_position_offset = self._append_to_active_file(
            stored_item=stored_item
        )
        self.key_dir.update(
            key=key,
            file_path=self.active_file.path,
            value_position=active_file_value_position_offset,
            value_size=stored_item.value_size,
            timestamp=stored_item.timestamp,
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

    def clear(self, delete_directory: bool = False) -> None:
        """Clears the storage space by deleting all the data files.
        The main purpose of this method is to be used to clean up after running tests.
        """
        for filename in os.listdir(self.directory):
            file_path = f"{self.directory}/{filename}"
            os.remove(file_path)
        if delete_directory:
            os.rmdir(self.directory)

    def rebuild_index(self):
        """Builds the key_dir index.

        The key_dir index is built by:
        - Reading all hint files (to speed up the index build)
        - Reading all data files that don't have a hint file associated
        - For each file read, adding the entry in the key_dir.

        This should be called at boot up.
        """
        data_files_without_hint_files, hint_files = self._get_index_rebuild_files()
        self.key_dir.rebuild(
            hint_files=hint_files, data_files=data_files_without_hint_files
        )
