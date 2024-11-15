from deltatable import DeltaTable
from helper import measure_rollback_time_and_size


def main():
    data = [
        {"name": "lucian", "role": "adc", "region": "runeterra", "species": "human"},
        {"name": "teemo", "role": "top", "region": "bandle", "species": "yordle"},
        {"name": "nasus", "role": "top", "region": "shurima", "species": "dog"},
    ]
    deltatable = DeltaTable(data)

    # Insert
    id = deltatable.insert(
        {"name": "jinx", "role": "adc", "region": "shurima", "species": "human"}
    )
    print(deltatable._db)
    # Update
    deltatable.update(
        id=id, row={"name": "jinx", "role": "adc", "region": "zaun", "species": "human"}
    )
    # Delete
    deltatable.delete(id=deltatable.select("name == 'teemo'").index[0])
    print(deltatable._db)
    deltatable.rollback_last_transaction()
    print(deltatable._db)
    deltatable.rollback_last_transaction()
    print(deltatable._db)
    deltatable.rollback_last_transaction()
    print(deltatable._db)

    final_data = [
        {"name": "lucian", "role": "adc", "region": "runeterra", "species": "human"},
        {"name": "nasus", "role": "top", "region": "shurima", "species": "dog"},
        {"name": "jinx", "role": "adc", "region": "zaun", "species": "human"},
    ]
    measure_rollback_time_and_size(deltatable)
    try:
        assert deltatable == DeltaTable(
            final_data
        ), "The final data should be equal to the expected data"
        print("Success!")
    except AssertionError as err:
        print("Assertion failed:", err)


if __name__ == "__main__":
    main()
