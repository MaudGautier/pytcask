import os

import pytest

from src.merge_worker import MergeWorker
from src.storage import Storage
from src.fixtures.database import (
    db_with_only_active_file,
    db_with_multiple_immutable_files,
    db_with_multiple_immutable_files_key_value_pairs,
)

TEST_DIRECTORY = "./datafiles/test"


@pytest.mark.parametrize("db_with_only_active_file", [TEST_DIRECTORY], indirect=True)
def test_can_append_and_retrieve_keys_in_one_file(db_with_only_active_file):
    # GIVEN
    database = db_with_only_active_file

    # WHEN
    value1 = database.get("key1")
    value2 = database.get("key2")

    # THEN
    assert value1 == b"yet_another_value1"
    assert value2 == b"value2"
    database.clear()


@pytest.mark.parametrize("db_with_only_active_file", [TEST_DIRECTORY], indirect=True)
def test_a_missing_key_returns_none(db_with_only_active_file):
    # GIVEN
    database = db_with_only_active_file

    # WHEN
    value1 = database.get("missing_key")

    # THEN
    assert value1 is None
    database.clear()


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_updates_and_retrievals_with_small_files(db_with_multiple_immutable_files):
    # GIVEN
    database = db_with_multiple_immutable_files

    # WHEN
    value1 = database.get("key1")
    value2 = database.get("key2")
    value3 = database.get("key3")

    # THEN
    assert value1 == b"yet_another_value1"
    assert value2 == b"another_value2"
    assert value3 == b"my_value3"
    database.clear()


def test_clear_database():
    # GIVEN
    directory = TEST_DIRECTORY
    database = Storage(directory=directory, max_file_size=15)
    database.append(key="key1", value=b"value1")
    database.append(key="key2", value=b"another_value2")
    database.append(key="key1", value=b"yet_another_value1")

    # WHEN/THEN
    all_files = os.listdir(directory)
    assert len(all_files) > 0
    database.clear()
    assert os.path.exists(directory) is True
    all_files = os.listdir(directory)
    assert len(all_files) == 0


def test_clear_database_and_directory():
    # GIVEN
    directory = TEST_DIRECTORY
    database = Storage(directory=directory, max_file_size=15)
    database.append(key="key1", value=b"yet_another_value1")

    # WHEN/THEN
    all_files = os.listdir(directory)
    assert len(all_files) > 0
    database.clear(delete_directory=True)
    assert os.path.exists(directory) is False


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_build_index(db_with_multiple_immutable_files):
    # GIVEN
    database = db_with_multiple_immutable_files
    assert len(os.listdir(database.directory)) == 5  # Check multiple files are present
    merge_worker = MergeWorker(storage=database, file_size_threshold=100)
    merge_worker.do_merge()

    # WHEN/THEN
    assert database.get(key="key1") == b"yet_another_value1"  # From hint file
    assert database.get(key="k3") == b"yet_another_val3"  # Still in active file
    # All others
    expected_pairs = {
        key: value for key, value in db_with_multiple_immutable_files_key_value_pairs
    }
    for key, expected_value in expected_pairs.items():
        assert database.get(key=key) == expected_value

    database.clear()


@pytest.mark.parametrize(
    "db_with_multiple_immutable_files", [TEST_DIRECTORY], indirect=True
)
def test_build_index_is_accessible_from_another_client(
    db_with_multiple_immutable_files,
):
    # GIVEN
    database = db_with_multiple_immutable_files
    assert len(os.listdir(database.directory)) == 5  # Check multiple files are present
    merge_worker = MergeWorker(storage=database, file_size_threshold=100)
    merge_worker.do_merge()
    database2 = Storage(directory=TEST_DIRECTORY, max_file_size=70)

    # WHEN
    value = database2.get(key="key1")

    # THEN
    assert value == b"yet_another_value1"

    database.clear()
