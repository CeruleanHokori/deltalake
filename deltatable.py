import pandas as pd
from dataclasses import dataclass
from helper import get_id, measure_rollback_time_and_size

@dataclass
class DeltaTable:
    _db: pd.DataFrame

    def __init__(self, data: list[dict] = None):
        if data is None:
            self._db = pd.DataFrame(columns=["ID"])
            return
        records = [
            {
            "ID": get_id(),
            **elt,
            }
            for elt in data
        ]
        self._db = pd.DataFrame(records)
        self._db.set_index("ID", inplace=True)

    def __eq__(self, dt):
        self_sorted = self._db.sort_values(by=self._db.columns.tolist()).reset_index(drop=True)
        df_sorted = dt._db.sort_values(by=dt._db.columns.tolist()).reset_index(drop=True)
        
        return self_sorted.equals(df_sorted)

    def insert(self, row: dict) -> int:
        row = {
        **row,
        "ID": get_id()
        }
        self._db = pd.concat([
        self._db,
        pd.DataFrame([row]).set_index(["ID"])
        ])
        return row["ID"]

    def delete(self, id: int):
        self._db = self._db.drop(index=id)

    def update(self, id: int, row: dict):
        self._db.loc[id] = row

    def select(self, condition) -> pd.DataFrame:
        return self._db.query(condition)

    def rollback_version(self, version: int):
        pass

    def rollback_transactions(self, number_of_transactions: int):
        pass
