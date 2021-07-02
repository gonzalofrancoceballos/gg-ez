from typing import Iterable, List, Union

import pymongo
from kedro.io import AbstractDataSet


class MongoDataSet(AbstractDataSet):
    """Handles i/o for data that is split into files inside of a folder"""

    def __init__(self, address: str, database_name: str, collection_name: str):
        self._address = address
        self._database_name = database_name
        self._collection_name = collection_name
        self._connection = pymongo.MongoClient(address)[database_name][collection_name]

    def _load(self, query: dict = None) -> List[dict]:
        query = query if query else {}
        data = self._connection.find(query)
        data = [doc for doc in data]
        return data

    def _save(self, data: Union[Iterable[dict], dict]) -> None:
        if isinstance(data, dict):
            self._connection.replace_one({"_id": data["_id"]}, data, upsert=True)
        elif isinstance(data, list):
            for doc in data:
                self._connection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
        else:
            raise ValueError("Data must be a `dict` or a `list` of `dict`")

    def _exists(self) -> bool:
        pass

    def _describe(self):
        return (
            f"MongoDB dataset: {self._address} "
            + f"| {self._database_name} | "
            + f"{self._collection_name}"
        )
