import os.path

import pytest

from src.fixtures.database import (
    db_with_only_active_file,
    db_with_only_active_file_key_value_pairs,
)
from src.io_handling import ReadableFile, ENCODING
from src.merge_worker import MergeWorker

TEST_DIRECTORY = "./datafiles/test_merger"


@pytest.mark.parametrize("db_with_only_active_file", [TEST_DIRECTORY], indirect=True)
def test_compact_keys_in_one_file(db_with_only_active_file):
    # GIVEN
    database = db_with_only_active_file
    merge_worker = MergeWorker(store_path=database.directory)
    file_to_merge = ReadableFile(database.active_file_path)

    # WHEN
    merged_file = merge_worker._merge_files(files=[file_to_merge])

    # THEN
    assert os.path.exists(file_to_merge.path) is False  # Merged file has been deleted
    assert len([entry for entry in merged_file]) == 4
    expected_values = {
        key: value for key, value in db_with_only_active_file_key_value_pairs
    }
    for entry in merged_file:
        assert expected_values[entry.key] == entry.value


# TODO: add tests when multiple files merged
# TODO: add test on do_merge public method
