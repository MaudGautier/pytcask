import os.path

import pytest

from src.__fixtures__.database import (
    db_with_only_active_file,
    db_with_only_active_file_key_value_pairs,
    db_with_multiple_immutable_files,
)
from src.io_handling import DataFile
from src.merge_worker import MergeWorker

TEST_DIRECTORY = "./datafiles/test_merger"


@pytest.mark.parametrize("db_with_only_active_file", [TEST_DIRECTORY], indirect=True)
def test_compact_keys_in_one_file(db_with_only_active_file):
    # GIVEN
    database = db_with_only_active_file
    merge_worker = MergeWorker(storage=database)
    file_to_merge = DataFile(database.active_data_file.path)

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
    merge_worker = MergeWorker(storage=database)

    # WHEN
    merge_worker.do_merge()

    # THEN — check that we have 3 files: the active one and the merged one with data and hint
    filenames = sorted(os.listdir(database.directory))
    assert len(filenames) == 3
    assert filenames[0] == "active.data"
    assert filenames[1].startswith("merged-") and filenames[1].endswith("data")
    assert filenames[2].startswith("merged-") and filenames[2].endswith("hint")

    database.clear()


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_can_retrieve_correct_key_from_merged_file(db_with_multiple_immutable_files):
    # GIVEN
    database = db_with_multiple_immutable_files
    assert len(os.listdir(database.directory)) == 5  # Check multiple files are present
    merge_worker = MergeWorker(storage=database)

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
    merge_worker = MergeWorker(storage=database)

    # WHEN
    mergeable_files = merge_worker._get_mergeable_files()

    # THEN
    assert database.active_data_file not in mergeable_files

    database.clear()


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_creates_new_merge_file_when_full(db_with_multiple_immutable_files):
    # GIVEN
    database = db_with_multiple_immutable_files
    assert len(os.listdir(database.directory)) == 5  # Check multiple files are present
    merge_worker = MergeWorker(storage=database, file_size_threshold=100)

    # WHEN
    merge_worker.do_merge()

    # THEN - ensure that we have multiple merged files
    merged_filenames = [
        name
        for name in os.listdir(database.directory)
        if name.startswith("merged-") and name.endswith("data")
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


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_hint_files_are_not_processed_by_merge_worker(db_with_multiple_immutable_files):
    # GIVEN
    database = db_with_multiple_immutable_files
    assert len(os.listdir(database.directory)) == 5  # Check multiple files are present
    merge_worker = MergeWorker(storage=database, file_size_threshold=100)

    # WHEN
    merge_worker.do_merge()

    # THEN
    files_to_merge = merge_worker._get_mergeable_files()
    assert len(files_to_merge) == 2
    for file in files_to_merge:
        assert not file.path.endswith(".hint")

    database.clear()


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_merge_process_uses_both_merged_and_new_immutable_files(
    db_with_multiple_immutable_files,
):
    # GIVEN
    database = db_with_multiple_immutable_files
    assert len(os.listdir(database.directory)) == 5  # Check multiple files are present
    merge_worker = MergeWorker(storage=database, file_size_threshold=100)

    # WHEN
    merge_worker.do_merge()
    database.append(key="new_key1", value=b"new_value1")
    database.append(key="new_key2", value=b"new_value2")
    database.append(key="new_key3", value=b"new_value3")
    database.append(key="new_key4", value=b"new_value4")
    database.append(key="new_key5", value=b"new_value5")

    # THEN
    files_to_merge = merge_worker._get_mergeable_files()
    assert len(files_to_merge) == 4
    already_merged = [file for file in files_to_merge if "merged-" in file.path]
    assert len(already_merged) == 2
    assert database.get(key="new_key3") == b"new_value3"
    assert database.get(key="key1") == b"yet_another_value1"

    database.clear()
