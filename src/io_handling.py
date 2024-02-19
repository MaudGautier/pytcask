import os
from typing import BinaryIO

from src.database_item import DatabaseItem
from src.io_utils import encode, decode


class File:
    Offset = int
    KEY_VALUE_PAIR_SEPARATOR = "\n"

    def __init__(self, path: str, mode: str):
        self.path = path
        self.file: BinaryIO = self.get_file(mode=mode)

    @staticmethod
    def read(path: str, start: int, end: int):
        with open(path, "rb") as file:
            file.seek(start)
            value = file.read(end - start)
            return decode(value)

    @staticmethod
    def ensure_directory_exists(file_path) -> None:
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def get_file(self, mode: str) -> BinaryIO:
        """Opens or creates a file in binary mode (depending on the mode passed)"""
        self.ensure_directory_exists(self.path)
        return open(self.path, mode=f"{mode}b")


class ActiveFile(File):
    def __init__(self, path: str):
        super().__init__(path=path, mode="w")

    def _append(self, item: DatabaseItem) -> File.Offset:
        self.file.write(encode(item.metadata))
        self.file.write(encode(item.key))
        value_position_offset = self._current_offset
        self.file.write(encode(item.value))
        self.file.write(encode(self.KEY_VALUE_PAIR_SEPARATOR))
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
