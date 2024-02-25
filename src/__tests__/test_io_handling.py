import pytest

from src.__fixtures__.database import (
    db_with_only_active_file_key_value_pairs,
    db_with_only_active_file,
)
from src.io_handling.data_file import DataFileItem, DataFile

TEST_DIRECTORY = "./datafiles/test_io_handling"


def test_can_decode_encoded_data():
    # GIVEN
    in_data_file_item = DataFileItem(key="key", value=b"value")
    assert in_data_file_item.key_size == 3
    assert in_data_file_item.value_size == 5
    in_bytes = in_data_file_item.to_bytes()

    # WHEN
    out_data_file_item = DataFileItem.from_bytes(in_bytes)

    # THEN
    assert out_data_file_item == in_data_file_item


@pytest.mark.parametrize("db_with_only_active_file", [TEST_DIRECTORY], indirect=True)
def test_can_iterate_on_file_and_decode_item(db_with_only_active_file):
    # GIVEN
    database = db_with_only_active_file
    file = DataFile(database.active_data_file.path)

    # WHEN/THEN (test __iter__ dunder method)
    i = 0
    for item in file:
        assert item.key == db_with_only_active_file_key_value_pairs[i][0]
        assert item.value == db_with_only_active_file_key_value_pairs[i][1]
        i += 1
