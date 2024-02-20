import os
import struct
from datetime import time, datetime
from functools import lru_cache
from typing import BinaryIO

from src.item import Item
from src.io_utils import encode, decode

ENCODING = "utf-8"
NB_BYTES_INTEGER = 4


class Storable:
    def __init__(
        self,
        key: str,
        value: bytes,
        timestamp: int = int(datetime.timestamp(datetime.now())),
    ):
        self.key = key
        self.value = value
        self.timestamp = timestamp

    @property
    def value_size(self) -> int:
        # Offset in bytes = number of bytes (because self.value is in bytes)
        return len(self.value)

    @property
    def key_size(self) -> int:
        return len(self.key)

    @property
    def timestamp_size(self) -> int:
        return 4  # should be 4 bytes i.e. 32 bits

    @property
    def human_timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp)

    @property
    def encoded_metadata(self) -> bytes:
        return struct.pack("iii", self.timestamp, self.key_size, self.value_size)

    @property
    def encoded_key(self):
        return bytes(self.key, encoding=ENCODING)

    @property
    def size(self) -> int:
        return len(self.encoded_metadata) + len(self.encoded_key) + len(self.value)

    def to_bytes(self) -> bytes:
        encoded_metadata = self.encoded_metadata
        encoded_key = self.encoded_key
        encoded_value = self.value

        return encoded_metadata + encoded_key + encoded_value

    @classmethod
    def from_bytes(cls, data: bytes):
        # metadata_offset is the number of bytes expected in the metadata
        metadata_offset = 3 * NB_BYTES_INTEGER
        timestamp, key_size, value_size = struct.unpack("iii", data[:metadata_offset])
        key = str(data[metadata_offset : metadata_offset + key_size], encoding=ENCODING)
        value = data[
            metadata_offset + key_size : metadata_offset + key_size + value_size
        ]

        return cls(key=key, value=value, timestamp=timestamp)

    def __eq__(self, other):
        return (
            self.key == other.key
            and self.value == other.value
            and self.timestamp == other.timestamp
        )

    def __repr__(self):
        return f"{self.key}:{self.value.decode(ENCODING)} ({self.timestamp})"

    @classmethod
    def from_item(cls, item: Item):
        return cls(value=bytes(item.value, encoding=ENCODING), key=item.key)


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

    def _append(self, item: Item) -> File.Offset:
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

    def append(self, item: Item) -> File.Offset:
        return self._append(item=item)

    def close(self) -> None:
        self.file.close()

    def convert_to_immutable(self, new_path: str) -> None:
        self.file.close()
        os.rename(src=self.path, dst=new_path)


class ImmutableFile(File):
    def __init__(self, path: str):
        super().__init__(path=path, mode="r")
