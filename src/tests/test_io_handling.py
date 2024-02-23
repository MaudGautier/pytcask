import pytest

from src.fixtures.database import (
    db_with_only_active_file_key_value_pairs,
    db_with_only_active_file,
)
from src.io_handling import StoredItem, ReadableFile

TEST_DIRECTORY = "./datafiles/test_io_handling"


def test_can_decode_encoded_data():
    # GIVEN
    in_stored_item = StoredItem(key="key", value=b"value")
    assert in_stored_item.key_size == 3
    assert in_stored_item.value_size == 5
    in_bytes = in_stored_item.to_bytes()

    # WHEN
    out_stored_item = StoredItem.from_bytes(in_bytes)

    # THEN
    assert out_stored_item == in_stored_item


@pytest.mark.parametrize("db_with_only_active_file", [TEST_DIRECTORY], indirect=True)
def test_can_iterate_on_file_and_decode_item(db_with_only_active_file):
    # GIVEN
    database = db_with_only_active_file
    file = ReadableFile(database.active_file_path)

    # WHEN/THEN (test __iter__ dunder method)
    i = 0
    for item in file:
        assert item.key == db_with_only_active_file_key_value_pairs[i][0]
        assert item.value == db_with_only_active_file_key_value_pairs[i][1]
        i += 1
