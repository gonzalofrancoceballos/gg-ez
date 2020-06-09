import os
import json
import yaml
from pathlib import Path
from typing import Union, Dict
import lightgbm as lgm
from utilities.utils import apply_regex_filter


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


def load_lightgbm_model(model_path: Path) -> lgm.Booster:
    """
    Load LightGBM model from path

    :param model_path: path to model

    :return: LighGBM model
    """

    return lgm.Booster(model_file=str(model_path))


def save_lightgbm_model(model: lgm.Booster, path: Union[str, Path]):
    """
    Save LightGBM model

    :param model: LightGBM model
    :param path: path to save model
    """

    model.save_model(str(path))


def load_lightgbm_models_quantile(
    model_quantile_path: Union[str, Path]
) -> Dict[float, lgm.Booster]:
    """
    Loads all quantile models existing in a given path

    :param model_quantile_path:

    :return: disctionary containing models
    """

    model_files = os.listdir(model_quantile_path)
    model_files = apply_regex_filter(model_files, "^model_")

    models = {}
    for model_file in model_files:
        q = float(model_file.split("_")[-1].split(".")[0]) / 100
        model_quantile = load_lightgbm_model(model_quantile_path / model_file)
        models[q] = model_quantile

    return models
