ENCODING = "utf-8"


def encode(content: str, encoding=ENCODING) -> bytes:
    return content.encode(encoding)


def decode(content: bytes, encoding=ENCODING) -> str:
    return content.decode(encoding)
