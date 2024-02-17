from src.database import Database

# ~~~~~~~~~~~~~~~~~~~
# ~~~ Temporary tests
# ~~~~~~~~~~~~~~~~~~~

if __name__ == "__main__":
    print("Starting !")
    database = Database()
    database.append(key="key1", value="value1")
    database.append(key="key2", value="value2")
    database.append(key="key3", value="value3")
    database.append(key="key4", value="value4")
    searched_keys = ["key1", "key2", "key3", "key4"]
    for searched_key in searched_keys:
        print(f"Here is the value of {searched_key}", database.get(searched_key))
    database.append(key="key1", value="another_value1")
    database.append(key="key2", value="another_value2")
    database.append(key="key3", value="another_value3")
    database.append(key="key1", value="yet_another_value1")
    searched_keys = ["key1", "key2", "key3", "key4"]
    for searched_key in searched_keys:
        print(f"Here is the value of {searched_key}", database.get(searched_key))

    missing_key = "i_dont_exist"
    print(f"Here is the value of {missing_key}", database.get(missing_key))
