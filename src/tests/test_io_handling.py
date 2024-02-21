import pytest

from src.fixtures.database import (
    db_with_only_active_file_key_value_pairs,
    db_with_only_active_file,
)
from src.io_handling import Storable, File, ENCODING

TEST_DIRECTORY = "./datafiles/test_io_handling"


def test_can_decode_encoded_data():
    in_storable = Storable(key="key", value=b"value")
    assert in_storable.key_size == 3
    assert in_storable.value_size == 5

    in_bytes = in_storable.to_bytes()

    out_storable = Storable.from_bytes(in_bytes)

    assert out_storable == in_storable


@pytest.mark.parametrize("db_with_only_active_file", [TEST_DIRECTORY], indirect=True)
def test_tmp(db_with_only_active_file):
    database = db_with_only_active_file
    file = File(
        database.active_file_path, mode="r"
    )  # TODO: should not have to pass mode I think
    i = 0
    for item in file:
        assert item.key == db_with_only_active_file_key_value_pairs[i][0]
        assert item.value == bytes(
            db_with_only_active_file_key_value_pairs[i][1], encoding=ENCODING
        )
        i += 1
