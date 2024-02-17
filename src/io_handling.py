import os

from src.database_item import DatabaseItem


class File:
    Offset = int
    TIMESTAMP_LENGTH = 10  # Assuming we don't use this DB later than year 2286 😁
    KEY_VALUE_PAIR_SEPARATOR = "\n"

    def __init__(self, path: str, mode: str):
        self.file = open(path, mode=mode)
        self.path = path

    @staticmethod
    def read(path: str, start: int, end: int):
        with open(path, "r") as file:
            file.seek(start)
            value = file.read(end - start)
            return value


class ActiveFile(File):
    def __init__(self, path: str):
        super().__init__(path=path, mode="w")

    def _append(self, item: DatabaseItem) -> File.Offset:
        self.file.write(item.metadata)
        self.file.write(item.key)
        value_position_offset = self._current_offset
        self.file.write(item.value)
        self.file.write(self.KEY_VALUE_PAIR_SEPARATOR)
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
