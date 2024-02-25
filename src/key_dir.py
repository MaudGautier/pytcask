from collections import namedtuple
from typing import Iterator

from src.item import Item


class KeyDir:
    KeyDirEntry = namedtuple(
        "KeyDirEntry", ["file_path", "value_position", "value_size", "timestamp"]
    )

    def __init__(self):
        self.entries = {}

    def __iter__(self) -> Iterator[KeyDirEntry]:
        return iter(zip(self.entries.keys(), self.entries.values()))

    def _clear(self):
        self.entries = {}

    def update(
        self,
        key: Item.Key,
        file_path: str,
        value_position: int,  # TO avoid circular import # TODO: improve ??? File.Offset,
        value_size: int,
        timestamp: int,
    ) -> None:
        self.entries[key] = self.KeyDirEntry(
            file_path=file_path,
            value_position=value_position,
            value_size=value_size,
            timestamp=timestamp,
        )

    def update_file_path(self, previous_path: str, new_path: str) -> None:
        for key, key_dir_entry in self:
            if key_dir_entry.file_path == previous_path:
                self.update(
                    key=key,
                    file_path=new_path,
                    value_position=key_dir_entry.value_position,
                    value_size=key_dir_entry.value_size,
                    timestamp=key_dir_entry.timestamp,
                )

    def get(self, key: Item.Key) -> KeyDirEntry or None:
        return self.entries[key] if key in self.entries else None

    # TODO: handle type without circular error
    def rebuild(self, hint_files, data_files):
        self._clear()
        for hint_file in hint_files:  # This is a HintFileItem
            for item in hint_file:
                self.update(
                    key=item.key,
                    file_path=hint_file.merged_file_path,
                    value_position=item.value_position,
                    value_size=item.value_size,
                    timestamp=item.timestamp,
                )
        for unmerged_data_file in data_files:
            for item in unmerged_data_file:  # This is a StoredItem
                self.update(
                    key=item.key,
                    file_path=unmerged_data_file.path,
                    value_position=item.value_position,
                    value_size=item.value_size,
                    timestamp=item.timestamp,
                )
