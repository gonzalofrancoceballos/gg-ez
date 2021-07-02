import json

import pandas as pd


class JSONHandler:
    """
    Parses json from buffer into dict or pd.DataFrame
    """

    def __init__(self, api_connector, **kwargs):
        self.connector = api_connector
        self.kwargs = kwargs

    def get_json(self, path: str, **kwargs) -> dict:
        """
        Parse into dict
        """
        merged_kwargs = self._merge(kwargs)
        with self.connector.read(path) as buffer:
            data = json.load(buffer, **merged_kwargs)
        return data

    def get_table(self, path: str, subset: list = None, **kwargs):
        """
        Parse into table
        """
        if not subset:
            subset = []

        with self.connector.read(path) as response:
            data = json.load(response)
            if subset is not None:
                data = self._access_dictionary_subset(data, subset)
        return pd.DataFrame(data, **kwargs)

    def save(self, item: dict, path: str, **kwargs):
        with self.connector.write(path) as buffer:
            json.dump(item, buffer)

    def _merge(self, kwargs):
        new_kwargs = self.kwargs.copy()
        new_kwargs.update(kwargs)
        return new_kwargs

    @staticmethod
    def _access_dictionary_subset(dictionary, subset):
        result = dictionary
        for k in subset:
            result = result.get(k)

        return result
