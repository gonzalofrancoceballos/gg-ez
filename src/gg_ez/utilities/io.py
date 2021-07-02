import json
from pathlib import Path
from typing import Union

import yaml


def read_json(file: Union[str, Path]) -> dict:
    """
    Read JSON file into dictionary

    :param file: path to file

    :return: dictionary containing JSON
    """

    with open(file, encoding="utf-8") as f:
        data = json.load(f)

    return data


def save_json(dict_object: dict, file: Union[str, Path]):
    """
    Save dictionary into JSON file

    :param dict_object: dictionary to save
    :param file: path to save JSON file
    """

    with open(file, "w") as f:
        json.dump(dict_object, f)


def read_yaml(file_path: Union[str, Path]) -> dict:
    """
    Read YAML file into dictionary

    :param file_path: path to file

    :return: dictionary containing JSON
    """

    with open(file_path) as file:
        dict_object = yaml.load(file, Loader=yaml.FullLoader)
    return dict_object


def save_yaml(dict_object: dict, file_path: Union[str, Path]):
    """
    Save dictionary into JSON file

    :param dict_object: dictionary to save
    :param file_path: path to save YAML file
    """

    with open(file_path, "w") as file:
        yaml.dump(dict_object, file)
