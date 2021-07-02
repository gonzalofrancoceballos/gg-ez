from abc import abstractmethod
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from gg_ez.utilities.io import read_json, save_json


class FolderData:
    """Handles data that is split into files inside of a folder"""

    def __init__(
        self,
        paths_dict: Dict[Any, Path],
        data: Dict[Any, Any] = None,
        lazy: bool = True,
    ):
        self.paths_dict = paths_dict
        self._data = data
        if not data and not lazy:
            self._load()

    def get_data(self):
        if not self._data:
            self._load()
        return self._data

    @abstractmethod
    def _load(self):
        pass

    @abstractmethod
    def save(self, **kwargs):
        pass


class JSONFolderData(FolderData):
    """Handles data that is split into JSON files inside of a folder"""

    def _load(self):
        self._data = {k: read_json(self.paths_dict[k]) for k in self.paths_dict.keys()}

    def save(self):
        for k in self.paths_dict.keys():
            save_json(self._data[k], self.paths_dict[k])


class CSVFolderData(FolderData):
    """Handles data that is split into CSV files inside of a folder"""

    def _load(self):
        self._data = {
            k: pd.read_csv(self.paths_dict[k]) for k in self.paths_dict.keys()
        }

    def save(self, **kwargs):
        for k in self.paths_dict.keys():
            self._data[k].to_csv(self.paths_dict[k], **kwargs)


class ExcelFolderData(FolderData):
    """Handles data that is split into EXCEL files inside of a folder"""

    def _load(self):
        self._data = {
            k: pd.read_excel(self.paths_dict[k]) for k in self.paths_dict.keys()
        }

    def save(self, **kwargs):
        for k in self.paths_dict.keys():
            self._data[k].to_excel(self.paths_dict[k], **kwargs)


class HDFFolderData(FolderData):
    """Handles data that is split into EXCEL files inside of a folder"""

    def _load(self):
        self._data = {
            k: pd.read_hdf(self.paths_dict[k]) for k in self.paths_dict.keys()
        }

    def save(self, **kwargs):
        for k in self.paths_dict.keys():
            self._data[k].to_hdf(self.paths_dict[k], **kwargs, key=str(k))
