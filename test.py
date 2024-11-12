from deltatable import DeltaTable
import pandas as pd

def main():
    data = [
        {"name": "lucian", "role": "adc", "region": "runeterra", "species": "human"},
        {"name": "teemo", "role": "top", "region": "bandle", "species": "yordle"},
        {"name": "nasus", "role": "top", "region": "shurima", "species": "dog"},
    ]
    deltatable = DeltaTable(data)

    # Insert
    id = deltatable.insert({"name": "jinx", "role": "adc", "region": "shurima", "species": "human"})
    # Update
    deltatable.update(id=id, row={"name": "jinx", "role": "adc", "region": "zaun", "species": "human"})
    # Delete
    deltatable.delete(id=deltatable.select("name == 'teemo'").index[0])

    final_data = [
        {"name": "lucian", "role": "adc", "region": "runeterra", "species": "human"},
        {"name": "nasus", "role": "top", "region": "shurima", "species": "dog"},
        {"name": "jinx", "role": "adc", "region": "zaun", "species": "human"},
    ]
    try:
        assert deltatable == DeltaTable(final_data), "The final data should be equal to the expected data"
        print("Success!")
    except AssertionError as err:
        print("Assertion failed:", err)

if __name__ == "__main__":
    main()