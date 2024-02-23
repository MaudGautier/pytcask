import pytest

from src.storage_engine import StorageEngine

db_with_only_active_file_key_value_pairs = [
    ("key1", b"value1"),
    ("key2", b"value2"),
    ("key3", b"my_value3"),
    ("key1_bis", b"value1_bis"),
    ("key1", b"another_value1"),
    ("key1", b"yet_another_value1"),
    ("key1_bis", b"another_value1_bis"),
]


@pytest.fixture
def db_with_only_active_file(request):
    database = StorageEngine(directory=request.param, max_file_size=1000)
    for key, value in db_with_only_active_file_key_value_pairs:
        database.append(key=key, value=value)
    return database


db_with_multiple_immutable_files_key_value_pairs = [
    ("key1", b"value1"),
    ("key2", b"value2"),
    ("key3", b"my_value3"),
    ("key1", b"another_value1"),
    ("key1", b"yet_another_value1"),
    ("key1_bis", b"another_value1_bis"),
    ("key2", b"another_value2"),
    ("k3", b"val3"),
    ("k3", b"another_val3"),
    ("k2", b"v2"),
    ("k3", b"yet_another_val3"),
]


@pytest.fixture
def db_with_multiple_immutable_files(request):
    database = StorageEngine(directory=request.param, max_file_size=70)
    for key, value in db_with_multiple_immutable_files_key_value_pairs:
        database.append(key=key, value=value)
    return database
