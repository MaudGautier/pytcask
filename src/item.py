class Item:
    Key = str
    Value = bytes

    def __init__(self, key: Key, value: Value):
        self.key = key
        self.value = value
