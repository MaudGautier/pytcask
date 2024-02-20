from time import time

from src.io_utils import encode


class Item:
    Key = str
    Value = str

    def __init__(self, key: Key, value: Value):
        self.key = key
        self.value = value

    @property
    def crc(self) -> str:
        return ""

    @property
    def key_size(self) -> int:
        return len(self.key)

    @property
    def value_size(self) -> int:
        return len(encode(self.value))

    @property
    def metadata(self) -> str:
        return f"{self.crc}{int(time())}{self.key_size}{self.value_size}"
