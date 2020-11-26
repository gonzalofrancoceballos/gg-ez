from kedro.io import AbstractDataSet
from gg_ez.api.handlers import JSONHandler
from gg_ez.api.connector import RapidApiConnector


class RapidAPIDataSet(AbstractDataSet):
    """Handles i/o for data that is split into files inside of a folder"""

    def __init__(self, credentials: dict):
        self._handler = JSONHandler(RapidApiConnector(credentials["token"]))

    def _load(self) -> callable:
        return self._handler.get_json

    def _save(self, data):
        raise NotImplementedError("RapidAPIDataset only to be used to load data")

    def _exists(self) -> bool:
        pass

    def _describe(self):
        return f"RapidAPIDataSet"
