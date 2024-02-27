import os
import struct
from datetime import datetime
from typing import Iterator

from src.io_handling.generic_file import ENCODING, NB_BYTES_INTEGER, File
from src.item import Item, Tombstone
from src.key_dir import KeyDir


class DataFileItem:
    def __init__(
        self,
        key: str,
        value: bytes or None,  # `None` is only in the case where `is_tombstone` is True
        timestamp: int = int(datetime.timestamp(datetime.now())),
        is_tombstone: bool = False,
    ):
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.is_tombstone = is_tombstone

    def __eq__(self, other) -> bool:
        return (
            self.key == other.key
            and self.value == other.value
            and self.timestamp == other.timestamp
        )

    def __repr__(self) -> str:
        return f"{self.key}:{self.value.decode(ENCODING)} ({self.timestamp})"

    @property
    def value_size(self) -> int:
        if self.is_tombstone:
            return 0
        # Offset in bytes = number of bytes (because self.value is in bytes)
        return len(self.value)

    @property
    def value_position(self) -> int:
        return len(self.encoded_metadata) + len(self.encoded_key)

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
    def encoded_key(self) -> bytes:
        return bytes(self.key, encoding=ENCODING)

    @property
    def size(self) -> int:
        return len(self.encoded_metadata) + len(self.encoded_key) + self.value_size

    @property
    def encoded_item(self) -> bytes:
        return self.to_bytes()

    def to_bytes(self) -> bytes:
        encoded_metadata = self.encoded_metadata
        encoded_key = self.encoded_key
        encoded_value = self.value

        if self.is_tombstone:
            return encoded_metadata + encoded_key

        return encoded_metadata + encoded_key + encoded_value

    @classmethod
    def from_bytes(cls, data: bytes) -> "DataFileItem":
        # metadata_offset is the number of bytes expected in the metadata
        metadata_offset = 3 * NB_BYTES_INTEGER
        timestamp, key_size, value_size = struct.unpack("iii", data[:metadata_offset])
        key = str(data[metadata_offset : metadata_offset + key_size], encoding=ENCODING)
        value = data[
            metadata_offset + key_size : metadata_offset + key_size + value_size
        ]
        is_tombstone = value_size == 0

        return cls(key=key, value=value, timestamp=timestamp, is_tombstone=is_tombstone)

    @classmethod
    def from_item(cls, item: Item) -> "DataFileItem":
        return cls(key=item.key, value=item.value)

    @classmethod
    def from_tombstone(cls, tombstone: Tombstone) -> "DataFileItem":
        return cls(key=tombstone.key, is_tombstone=True, value=None)


class DataFile(File):
    def __init__(self, path: str, read_only: bool = True):
        super().__init__(path=path, mode="r" if read_only else "w")

    def __iter__(self, item_class=DataFileItem) -> Iterator[DataFileItem]:
        return super().__iter__(item_class=item_class)


class ImmutableDataFile(DataFile):
    def __init__(self, path: str):
        super().__init__(path=path, read_only=True)


class WritableDataFile(DataFile):
    def __init__(self, path: str):
        super().__init__(path=path, read_only=False)


class MergedDataFile(WritableDataFile):
    def __init__(self, store_path: str):
        # Using timestamp in nanoseconds to avoid name collisions
        timestamp_in_ns = int(datetime.timestamp(datetime.now()) * 1_000_000)
        file_path = f"{store_path}/merged-{timestamp_in_ns}.data"
        super().__init__(path=file_path)

    def write(self, data_file_items: list[DataFileItem]) -> KeyDir:
        file_key_dir = KeyDir()
        offset = 0
        for data_file_item in data_file_items:
            nb_bytes_written = self.file.write(data_file_item.encoded_item)
            file_key_dir.update(
                file_path=self.path,
                value_size=data_file_item.value_size,
                value_position=offset + data_file_item.value_position,
                key=data_file_item.key,
                timestamp=data_file_item.timestamp,
            )
            offset += nb_bytes_written

        return file_key_dir


class ActiveDataFile(WritableDataFile):
    def _append(self, data_file_item: DataFileItem) -> File.Offset:
        self.file.write(data_file_item.to_bytes())
        offset = self.file.tell()
        # WARNING: The following leaks info from storable to file which is not great
        value_position_offset = offset - data_file_item.value_size
        self.file.flush()
        return value_position_offset

    @property
    def _current_offset(self) -> File.Offset:
        return self.file.tell()

    @property
    def size(self) -> File.Offset:
        return self._current_offset

    def append(self, data_file_item: DataFileItem) -> File.Offset:
        return self._append(data_file_item=data_file_item)

    def close(self) -> None:
        self.file.close()

    def convert_to_immutable(self, new_path: str) -> None:
        self.file.close()
        os.rename(src=self.path, dst=new_path)
