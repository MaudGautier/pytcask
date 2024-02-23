from collections import namedtuple

from src.item import Item
from src.io_handling import File


class KeyDir:
    KeyDirEntry = namedtuple(
        "KeyDirEntry", ["file_path", "value_position", "value_size"]
    )

    def __init__(self):
        self.entries = {}

    def update(
        self,
        key: Item.Key,
        file_path: str,
        value_position: File.Offset,
        value_size: int,
    ) -> None:
        self.entries[key] = self.KeyDirEntry(
            file_path=file_path, value_position=value_position, value_size=value_size
        )

    def update_file_path(self, previous_path: str, new_path: str) -> None:
        for key, key_dir_entry in self.entries.items():
            if key_dir_entry.file_path == previous_path:
                self.update(
                    key=key,
                    file_path=new_path,
                    value_position=key_dir_entry.value_position,
                    value_size=key_dir_entry.value_size,
                )

    def get(self, key: Item.Key) -> KeyDirEntry or None:
        return self.entries[key] if key in self.entries else None
