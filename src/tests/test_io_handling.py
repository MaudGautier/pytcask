from src.io_handling import Storable


def test_can_decode_encoded_data():
    in_storable = Storable(key="key", value=b"value")
    assert in_storable.key_size == 3
    assert in_storable.value_size == 5

    in_bytes = in_storable.to_bytes()

    out_storable = Storable.from_bytes(in_bytes)

    assert out_storable == in_storable
