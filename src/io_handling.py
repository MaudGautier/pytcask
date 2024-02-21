import os
import struct
from datetime import datetime
from typing import BinaryIO, Iterator

from src.item import Item

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
    def encoded_key(self) -> bytes:
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
    def from_bytes(cls, data: bytes) -> "Storable":
        # metadata_offset is the number of bytes expected in the metadata
        metadata_offset = 3 * NB_BYTES_INTEGER
        timestamp, key_size, value_size = struct.unpack("iii", data[:metadata_offset])
        key = str(data[metadata_offset : metadata_offset + key_size], encoding=ENCODING)
        value = data[
            metadata_offset + key_size : metadata_offset + key_size + value_size
        ]

        return cls(key=key, value=value, timestamp=timestamp)

    def __eq__(self, other) -> bool:
        return (
            self.key == other.key
            and self.value == other.value
            and self.timestamp == other.timestamp
        )

    def __repr__(self) -> str:
        return f"{self.key}:{self.value.decode(ENCODING)} ({self.timestamp})"

    @classmethod
    def from_item(cls, item: Item) -> "Storable":
        return cls(value=bytes(item.value, encoding=ENCODING), key=item.key)


class File:
    Offset = int
    KEY_VALUE_PAIR_SEPARATOR = "\n"

    def __init__(self, path: str, mode: str):
        self.path = path
        self.file: BinaryIO = self.get_file(mode=mode)

    @staticmethod
    def read(path: str, start: int, end: int) -> str:
        with open(path, "rb") as file:
            file.seek(start)
            value = file.read(end - start)
            # The following is true only for string values (or could be value.decode(ENCODING) as well)
            # TODO: handle other cases as well (integer values for example) + add tests
            return str(value, encoding=ENCODING)

    @staticmethod
    def ensure_directory_exists(file_path) -> None:
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def get_file(self, mode: str) -> BinaryIO:
        """Opens or creates a file in binary mode (depending on the mode passed)"""
        self.ensure_directory_exists(self.path)
        return open(self.path, mode=f"{mode}b")

    def __iter__(self) -> Iterator[Storable]:
        file_size = os.path.getsize(self.path)
        with open(self.path, "rb") as file:
            # This means that the whole file is stored in memory at once. This is required because the size of the next
            # chunk depends on the size of the key and values (which we can't know before consuming the next bytes).
            # Another approach would have been to read a given chunk size that is larger than necessary (but smaller
            # than the whole file). But that would make the code much more complex and it is not necessary here.
            # So I opted for simplicity.
            data = file.read()
            offset = 0
            while offset < file_size:
                stored_item = Storable.from_bytes(data[offset:])
                chunk_size = stored_item.size
                offset += chunk_size
                yield stored_item

    def __lt__(self, other):
        """Files are sorted based on their creation date"""
        return os.path.getctime(self.path) < os.path.getctime(other.path)


class ActiveFile(File):
    def __init__(self, path: str):
        super().__init__(path=path, mode="w")

    def _append(self, storable: Storable) -> File.Offset:
        self.file.write(storable.to_bytes())
        offset = self.file.tell()
        # WARNING: The following leaks info from storable to file which is not great
        value_position_offset = offset - storable.value_size
        self.file.flush()
        return value_position_offset

    @property
    def _current_offset(self) -> File.Offset:
        return self.file.tell()

    @property
    def size(self) -> File.Offset:
        return self._current_offset

    def append(self, storable: Storable) -> File.Offset:
        return self._append(storable=storable)

    def close(self) -> None:
        self.file.close()

    def convert_to_immutable(self, new_path: str) -> None:
        self.file.close()
        os.rename(src=self.path, dst=new_path)


class ImmutableFile(File):
    def __init__(self, path: str):
        super().__init__(path=path, mode="r")
