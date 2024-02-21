import pytest

from src.storage_engine import StorageEngine

db_with_only_active_file_key_value_pairs = [
    ("key1", "value1"),
    ("key2", "value2"),
    ("key3", "my_value3"),
    ("key1_bis", "value1_bis"),
    ("key1", "another_value1"),
    ("key1", "yet_another_value1"),
    ("key1_bis", "another_value1_bis"),
]


@pytest.fixture
def db_with_only_active_file(request):
    database = StorageEngine(directory=request.param, max_file_size=1000)
    for key, value in db_with_only_active_file_key_value_pairs:
        database.append(key=key, value=value)
    return database
