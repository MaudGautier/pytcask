from collections import namedtuple

from src.database_item import DatabaseItem
from src.io_handling import File


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
