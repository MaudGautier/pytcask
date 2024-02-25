import os
import struct
from typing import Iterator

from src.io_handling.data_file import MergedDataFile
from src.io_handling.generic_file import File, ENCODING, NB_BYTES_INTEGER
from src.key_dir import KeyDir


# TODO: refactor HintFileItem and StoredItem (= DataFileItem) together ??? (many things similar)
class HintFileItem:
    def __init__(
        self,
        timestamp: int,
        value_size: int,
        key: str,
        value_position: int,
    ):
        self.timestamp = timestamp
        self.key = key
        self.value_size = value_size
        self.value_position = value_position
        self.key_size = len(self.key)

    def __repr__(self):
        return f"{self.key}: {self.timestamp}-{self.key_size}-{self.value_size}-{self.value_position}"

    @property
    def encoded_metadata(self) -> bytes:
        return struct.pack(
            "iiii", self.timestamp, self.key_size, self.value_size, self.value_position
        )

    @property
    def encoded_key(self) -> bytes:
        return bytes(self.key, encoding=ENCODING)

    @property
    def size(self):
        return len(self.encoded_metadata) + len(self.encoded_key)

    def to_bytes(self):
        metadata = self.encoded_metadata
        return metadata + bytes(self.key, encoding=ENCODING)

    @classmethod
    def from_bytes(cls, data: bytes) -> "HintFileItem":
        metadata_offset = 4 * NB_BYTES_INTEGER
        timestamp, key_size, value_size, value_position = struct.unpack(
            "iiii", data[:metadata_offset]
        )
        key = str(data[metadata_offset : metadata_offset + key_size], encoding=ENCODING)

        return cls(
            key=key,
            value_size=value_size,
            value_position=value_position,
            timestamp=timestamp,
        )


class HintFile(File):
    def __init__(self, path: str, read_only: bool = False):
        self.path = path
        super().__init__(path=self.path, mode="r" if read_only else "w")

    @property
    def merged_file_path(self):
        return os.path.splitext(self.path)[0] + ".data"

    @classmethod
    def from_merge_file(cls, merged_file: MergedDataFile):
        return cls(path=os.path.splitext(merged_file.path)[0] + ".hint")

    def write(self, merged_file_key_dir: KeyDir) -> None:
        for key, entry in merged_file_key_dir:
            item = HintFileItem(
                timestamp=entry.timestamp,
                value_size=entry.value_size,
                value_position=entry.value_position,
                key=key,
            )
            self.file.write(item.to_bytes())

    # TODO: This is a close copy-paste of the File.__iter__ dunder method.
    #  => Refactor with a generic ?? (StoredItem(DataFileItem) vs HintItem(HintFileItem))
    def __iter__(self) -> Iterator[HintFileItem]:
        file_size = os.path.getsize(self.path)
        with open(self.path, "rb") as file:
            data = file.read()
            offset = 0
            while offset < file_size:
                item = HintFileItem.from_bytes(data[offset:])
                chunk_size = item.size
                offset += chunk_size
                yield item
