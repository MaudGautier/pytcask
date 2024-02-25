import os
from enum import Enum
from typing import BinaryIO, Iterator

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

    def __lt__(self, other: "File"):
        """Files are sorted based on their creation date"""
        return os.path.getctime(self.path) < os.path.getctime(other.path)

    def __iter__(self, item_class) -> Iterator:
        file_size = os.path.getsize(self.path)
        with open(self.path, "rb") as file:
            # This means that the whole file is stored in memory at once. This is required because the size of the next
            # chunk depends on the size of the key and value (which we can't know before consuming the next bytes).
            # Another approach would have been to read a given chunk size that is larger than necessary (but smaller
            # than the whole file). But that would make the code much more complex, and it is not necessary here.
            # So I opted for simplicity.
            data = file.read()
            offset = 0
            while offset < file_size:
                data_file_item = item_class.from_bytes(data[offset:])
                chunk_size = data_file_item.size
                offset += chunk_size
                yield data_file_item

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
