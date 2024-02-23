import os.path

import pytest

from src.fixtures.database import (
    db_with_only_active_file,
    db_with_only_active_file_key_value_pairs,
    db_with_multiple_immutable_files,
)
from src.io_handling import ReadableFile
from src.merge_worker import MergeWorker

TEST_DIRECTORY = "./datafiles/test_merger"


@pytest.mark.parametrize("db_with_only_active_file", [TEST_DIRECTORY], indirect=True)
def test_compact_keys_in_one_file(db_with_only_active_file):
    # GIVEN
    database = db_with_only_active_file
    merge_worker = MergeWorker(
        store_path=database.directory, storage_engine=database
    )  # TODO: REMOVE STORE_PATH
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
    database.clear()


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_merge_multiple_files_results_in_one(db_with_multiple_immutable_files):
    # GIVEN
    database = db_with_multiple_immutable_files
    assert len(os.listdir(database.directory)) == 5  # Check multiple files are present
    merge_worker = MergeWorker(store_path=database.directory, storage_engine=database)

    # WHEN
    merge_worker.do_merge()

    # THEN — check that we have two files: the active one and the merged one
    filenames = sorted(os.listdir(database.directory))
    assert len(filenames) == 2
    assert filenames[0] == "active.txt"
    assert filenames[1].startswith("merged-")

    database.clear()


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_can_retrieve_correct_key_from_merged_file(db_with_multiple_immutable_files):
    # GIVEN
    database = db_with_multiple_immutable_files
    assert len(os.listdir(database.directory)) == 5  # Check multiple files are present
    merge_worker = MergeWorker(store_path=database.directory, storage_engine=database)

    # WHEN
    merge_worker.do_merge()
    value1 = database.get(key="key1")
    value2 = database.get(key="key2")
    value3 = database.get(key="key3")
    value4 = database.get(key="key1_bis")
    value5 = database.get(key="k2")
    value6 = database.get(key="k3")

    # THEN
    assert value1 == b"yet_another_value1"
    assert value2 == b"another_value2"
    assert value3 == b"my_value3"
    assert value4 == b"another_value1_bis"
    assert value5 == b"v2"
    assert value6 == b"yet_another_val3"

    database.clear()
