from src.storage_engine import StorageEngine


def test_can_append_and_retrieve_keys_in_one_file():
    # GIVEN
    database = StorageEngine(directory="./datafiles/test/", max_file_size=1000)
    database.append(key="key1", value="value1")
    database.append(key="key2", value="value2")
    database.append(key="key1", value="another_value1")
    database.append(key="key1", value="yet_another_value1")

    # WHEN
    value1 = database.get("key1")
    value2 = database.get("key2")

    # THEN
    assert value1 == "yet_another_value1"
    assert value2 == "value2"


def test_a_missing_key_returns_none():
    # GIVEN
    database = StorageEngine(directory="./datafiles/test/", max_file_size=1000)

    # WHEN
    value1 = database.get("key1")

    # THEN
    assert value1 is None


def test_updates_and_retrievals_with_small_files():
    # GIVEN
    database = StorageEngine(directory="./datafiles/test/", max_file_size=15)
    database.append(key="key1", value="value1")
    database.append(key="key2", value="value2")
    database.append(key="key3", value="value3")
    database.append(key="key1", value="another_value1")
    database.append(key="key2", value="another_value2")
    database.append(key="key1", value="yet_another_value1")

    # WHEN
    value1 = database.get("key1")
    value2 = database.get("key2")
    value3 = database.get("key3")

    # THEN
    assert value1 == "yet_another_value1"
    assert value2 == "another_value2"
    assert value3 == "value3"
