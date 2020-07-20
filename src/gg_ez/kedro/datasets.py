import os
from pathlib import Path
from kedro.io import AbstractDataSet
from typing import Dict, Any

from gg_ez.utilities.folder_data import (
    CSVFolderData,
    JSONFolderData,
    ExcelFolderData,
    HDFFolderData,
    FolderData,
)

DATA_FOLDER_OBJECTS = {
    "csv": CSVFolderData,
    "json": JSONFolderData,
    "xlsx": ExcelFolderData,
    "hdf": HDFFolderData,
    "h5": HDFFolderData,
}


class FolderDataDataset(AbstractDataSet):
    """Handles i/o for data that is split into files inside of a folder"""

    def __init__(
        self,
        filepath,
        data_format,
        suffix: str,
        id_list=None,
        lazy=True,
        save_args=None,
    ):
        self._filepath = Path(filepath)
        self._format = data_format
        self.folder_data_object = self._get_folder_data_object()
        self._suffix = suffix
        self._id_list = id_list
        self._lazy = lazy
        self._save_args = save_args

    def _load(self) -> FolderData:
        """Explores directory and returns a `FolderData` object containing all files"""
        if self._filepath.exists():
            files_in_path = os.listdir(self._filepath)
            files_in_path = [
                file for file in files_in_path if f".{self._format}" in file
            ]
            full_paths = [self._filepath / file for file in files_in_path]
            ids = [file.split(f"_{self._suffix}")[0] for file in files_in_path]
            paths_dict = {k: v for k, v in zip(ids, full_paths)}
        else:
            paths_dict = {}

        return self.folder_data_object(paths_dict, lazy=self._lazy)

    def _save(self, data: Dict[Any, Any]) -> None:
        os.makedirs(self._filepath, exist_ok=True)
        paths_dict = {
            k: self._filepath / f"{k}_{self._suffix}.{self._format}"
            for k in data.keys()
        }

        data_folder_object = self._get_folder_data_object()(
            paths_dict=paths_dict, data=data
        )
        data_folder_object.save(**self._save_args)

    def _exists(self) -> bool:
        return Path(self._filepath).exists()

    def _describe(self):
        return f"file_path: {self._filepath}"

    def _get_folder_data_object(self):
        if self._format in DATA_FOLDER_OBJECTS.keys():
            return DATA_FOLDER_OBJECTS[self._format]
        else:
            raise ValueError(f"{self._filepath} is not a valid format")
