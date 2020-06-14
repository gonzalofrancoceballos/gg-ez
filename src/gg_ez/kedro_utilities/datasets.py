import os
from pathlib import Path
from gg_ez.utilities.io import JSONData
from gg_ez.utilities.io import save_json
from kedro.io import AbstractDataSet
from typing import Dict, Any


class JSONFolderDataset(AbstractDataSet):
    """Datast for data that is split into JSON files inside of a folder"""

    def __init__(self, filepath, suffix: str, id_list=None, lazy=True):
        self._filepath = Path(filepath)
        self._suffix = suffix
        self._id_list = id_list
        self._lazy = lazy

    def _load(self) -> JSONData:
        if self._filepath.exists():
            files_in_path = os.listdir(self._filepath)
            files_in_path = [file for file in files_in_path if ".json" in file]
            full_paths = [self._filepath / file for file in files_in_path]
            ids = [file.split(f"_{self._suffix}")[0] for file in files_in_path]
            paths_dict = {k: v for k, v in zip(ids, full_paths)}
        else:
            paths_dict = {}

        return JSONData(paths_dict, lazy=self._lazy)

    def _save(self, json_data: Dict[Any, dict]) -> None:
        os.makedirs(self._filepath, exist_ok=True)
        for k in json_data.keys():
            save_json(json_data[k], self._filepath / f"{k}_{self._suffix}.json")

    def _exists(self) -> bool:
        return Path(self._filepath).exists()

    def _describe(self):
        return f"file_path: {self._filepath}"
