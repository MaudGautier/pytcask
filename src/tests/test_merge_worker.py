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
    merge_worker = MergeWorker(storage_engine=database)
    file_to_merge = ReadableFile(database.active_file.path)

    # WHEN
    merged_files = merge_worker._merge_files(files=[file_to_merge])

    # THEN
    assert os.path.exists(file_to_merge.path) is False  # Merged file has been deleted
    assert len([entry for merged_file in merged_files for entry in merged_file]) == 4
    expected_values = {
        key: value for key, value in db_with_only_active_file_key_value_pairs
    }
    for merged_file in merged_files:
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
    merge_worker = MergeWorker(storage_engine=database)

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
    merge_worker = MergeWorker(storage_engine=database)

    # WHEN
    merge_worker.do_merge()

    # THEN
    assert database.get(key="key1") == b"yet_another_value1"
    assert database.get(key="key2") == b"another_value2"
    assert database.get(key="key3") == b"my_value3"
    assert database.get(key="key1_bis") == b"another_value1_bis"
    assert database.get(key="k2") == b"v2"
    assert database.get(key="k3") == b"yet_another_val3"

    database.clear()


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_list_immutable_files_does_not_return_active_file(
    db_with_multiple_immutable_files,
):
    # GIVEN
    database = db_with_multiple_immutable_files
    merge_worker = MergeWorker(storage_engine=database)

    # WHEN
    immutable_files = merge_worker._list_all_immutable_files()

    # THEN
    assert database.active_file not in immutable_files

    database.clear()


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_creates_new_merge_file_when_full(db_with_multiple_immutable_files):
    # GIVEN
    database = db_with_multiple_immutable_files
    # assert len(os.listdir(database.directory)) == 5  # Check multiple files are present
    merge_worker = MergeWorker(storage_engine=database, max_file_size=100)

    # WHEN
    merge_worker.do_merge()

    # THEN - ensure that we have multiple merged files
    merged_filenames = [
        name for name in os.listdir(database.directory) if name.startswith("merged-")
    ]
    assert len(merged_filenames) == 2

    # THEN - ensure that we fetch the correct values
    assert database.get(key="key1") == b"yet_another_value1"
    assert database.get(key="key2") == b"another_value2"
    assert database.get(key="key3") == b"my_value3"
    assert database.get(key="key1_bis") == b"another_value1_bis"
    assert database.get(key="k2") == b"v2"
    assert database.get(key="k3") == b"yet_another_val3"

    database.clear()
