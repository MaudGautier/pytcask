class Item:
    Key = str
    Value = bytes

    def __init__(self, key: Key, value: Value):
        self.key = key
        self.value = value


class Tombstone:
    def __init__(self, key: Item.Key):
        self.key = key
