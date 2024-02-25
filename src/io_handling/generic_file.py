import os
from enum import Enum
from typing import BinaryIO


ENCODING = "utf-8"
NB_BYTES_INTEGER = 4


class FileType(str, Enum):
    HINT = "hint"
    MERGED_DATA = "merged_data"
    UNMERGED_DATA = "unmerged_data"


class File:
    Offset = int
    KEY_VALUE_PAIR_SEPARATOR = "\n"

    def __init__(self, path: str, mode: str):
        self.path = path
        self.file: BinaryIO = self._get_file(mode=mode)

    def __lt__(self, other):
        """Files are sorted based on their creation date"""
        return os.path.getctime(self.path) < os.path.getctime(other.path)

    @staticmethod
    def _ensure_directory_exists(file_path) -> None:
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def _get_file(self, mode: str) -> BinaryIO:
        """Opens or creates a file in binary mode (depending on the mode passed)"""
        self._ensure_directory_exists(self.path)
        return open(self.path, mode=f"{mode}b")

    @property
    def type(self) -> str:
        filename = os.path.basename(self.path)
        if filename.endswith(".hint"):
            return FileType.HINT
        if filename.endswith(".data") and filename.startswith("merged-"):
            return FileType.MERGED_DATA
        if filename.endswith(".data") and not filename.startswith("merged-"):
            return FileType.UNMERGED_DATA

    @staticmethod
    def read(path: str, start: int, end: int) -> bytes:
        with open(path, "rb") as file:
            file.seek(start)
            value = file.read(end - start)
            return value

    def discard(self):
        """Discards the file"""
        os.remove(self.path)

    def close(self):
        self.file.close()
