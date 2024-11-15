import copy
import sys
from dataclasses import dataclass

import pandas as pd

from helper import get_id


@dataclass
class Transaction:
    id: int
    method: str
    row_id: int
    row: dict


@dataclass
class CheckPoint:
    transactions: list[Transaction]
    db: pd.DataFrame

    @property
    def start_id(self):
        return self.transactions[0].id

    @property
    def end_id(self):
        return self.transactions[0].id

    @property
    def number_of_transactions(self):
        return len(self.transactions)


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
        self._transactions: list[Transaction] = []
        self._transaction_id: int = 0
        self._versions: dict[pd.DataFrame] = {}
        self._version_id: int = 0
        self._checkpoints: list[CheckPoint] = [
            CheckPoint(
                transactions=[],
                db=copy.deepcopy(self._db),
            )
        ]

    def __eq__(self, dt):
        self_sorted = self._db.sort_values(by=self._db.columns.tolist()).reset_index(
            drop=True
        )
        df_sorted = dt._db.sort_values(by=dt._db.columns.tolist()).reset_index(
            drop=True
        )

        return self_sorted.equals(df_sorted)

    def get_transactions(self) -> list[Transaction]:
        return self._transactions

    def get_versions(self) -> list["DeltaTable"]:
        return self._versions

    def get_checkpoints(self) -> list[CheckPoint]:
        return self._checkpoints

    def get_size_of_transactions(self):
        return sum([sys.getsizeof(trans) for trans in self._transactions])

    def get_size_of_versions(self):
        return sum([sys.getsizeof(version) for version in self._versions.values()])

    def get_size_of_checkpoints(self):
        return sum(
            [
                sys.getsizeof(check.db) + sys.getsizeof(check.transactions)
                for check in self._checkpoints
            ]
        )

    def add_transaction(self, method: str, row_id: int, row: dict):
        self._transactions.append(
            Transaction(
                id=self._transaction_id,
                method=method,
                row_id=row_id,
                row=row,
            )
        )
        self._transaction_id += 1
        self.get_checkpoints()[-1].transactions.append(self._transactions[-1])

    def add_version(self):
        self._versions[self._version_id] = copy.deepcopy(self._db)
        self._version_id += 1

    def add_checkpoint(self):
        self._checkpoints.append(
            CheckPoint(
                transactions=[],
                db=copy.deepcopy(self._db),
            )
        )

    def insert(
        self,
        row: dict,
        add_transaction: bool = True,
        add_version: bool = True,
        add_checkpoint=True,
    ) -> int:
        _id = get_id()
        row = {**row, "ID": _id}
        self._db = pd.concat([self._db, pd.DataFrame([row]).set_index(["ID"])])
        if add_transaction:
            self.add_transaction(
                method="insert",
                row_id=_id,
                row=row,
            )
        if add_version:
            self.add_version()
        if add_checkpoint and self._transaction_id % 50 == 0:
            self.add_checkpoint()
        return row["ID"]

    def delete(
        self,
        id: int,
        add_transaction: bool = True,
        add_version: bool = True,
        add_checkpoint=True,
    ):
        if add_transaction:
            self.add_transaction(
                method="delete",
                row_id=id,
                row=self._db.loc[id],
            )
        if add_version:
            self.add_version()
        if add_checkpoint and self._transaction_id % 50 == 0:
            self.add_checkpoint()
        self._db = self._db.drop(index=id)

    def update(
        self,
        id: int,
        row: dict,
        add_transaction: bool = True,
        add_version: bool = True,
        add_checkpoint=True,
    ):
        if add_transaction:
            self.add_transaction(
                method="update",
                row_id=id,
                row=self._db.loc[id],
            )
        if add_version:
            self.add_version()

        if add_checkpoint and self._transaction_id % 50 == 0:
            self.add_checkpoint()

        self._db.loc[id] = row

    def select(self, condition) -> pd.DataFrame:
        return self._db.query(condition)

    def reverse_transaction(self, transaction: Transaction) -> None:
        match transaction.method:
            case "delete":
                self.insert(
                    transaction.row,
                    add_transaction=False,
                    add_version=False,
                    add_checkpoint=False,
                )
            case "insert":
                self.delete(
                    transaction.row_id,
                    add_transaction=False,
                    add_version=False,
                    add_checkpoint=False,
                )
            case "update":
                self.update(
                    transaction.row_id,
                    transaction.row,
                    add_transaction=False,
                    add_version=False,
                    add_checkpoint=False,
                )

    def apply_transaction(self, transaction: Transaction) -> None:
        match transaction.method:
            case "delete":
                self.delete(
                    transaction.row_id,
                    add_transaction=False,
                    add_version=False,
                    add_checkpoint=False,
                )
            case "insert":
                self.insert(
                    transaction.row,
                    add_transaction=False,
                    add_version=False,
                    add_checkpoint=False,
                )
            case "update":
                self.update(
                    transaction.row_id,
                    transaction.row,
                    add_transaction=False,
                    add_version=False,
                    add_checkpoint=False,
                )

    def rollback_last_transaction(self):
        self.reverse_transaction(self._transactions[-1])
        self._transactions = [*self._transactions[:-1]]

    def rollback_version(self, version: int):
        self._db = self._versions[version]

    def rollback_transactions(self, number_of_transactions: int):
        for _ in range(number_of_transactions):
            self.rollback_last_transaction()

    def rollback_checkpoint(self, number_of_transactions: int):
        checkpoint = self.get_checkpoints()[-1]
        while checkpoint.number_of_transactions < number_of_transactions:
            self._checkpoints = [*self._checkpoints[:-1]]
            checkpoint = self.get_checkpoints()[-1]
            number_of_transactions -= checkpoint.number_of_transactions

        self._db = checkpoint.db
        for transaction in checkpoint.transactions:
            self.apply_transaction(transaction)
