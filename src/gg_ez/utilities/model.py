from typing import Any, List, Tuple, Union

import numpy as np
import pandas as pd
from lightgbm import Booster, Dataset


def to_log(x: Union[np.array, pd.Series]) -> np.array:
    """
    Applies log-like function

    Normally used to
        - Normalize a dataset with outliers
        - Avoid bigger contribution of big number to global loss function
        - Rescaling when plotting
    """

    return np.where(x >= 0, np.log(x + 1), -np.log(-x + 1))


def from_log(x: Union[np.array, pd.Series]) -> np.array:
    """
    Inverse transform os `to_log` function
    """
    return np.where(x >= 0, np.exp(x) - 1, -np.exp(-x) + 1)


def get_numeric_columns(df: pd.DataFrame) -> List[Any]:
    """
    Returns name of all numeric columns of a table

    :param df: table to analise

    :return: numeric columns
    """

    return df.select_dtypes(include=np.number).columns.tolist()


def get_valid_train_columns(
    master: pd.DataFrame, ignore_columns: list, target_column: str
) -> List[Any]:
    """
    Gets list of valid columns to use in train

    :param master: master table whose columns we want to get
    :param ignore_columns: columns to ignore
    :param target_column: name of target colyumn

    :return: list of columns to use in train
    """

    train_columns = get_numeric_columns(master)
    train_columns = list(set(train_columns) - set(ignore_columns + [target_column]))

    return train_columns


def get_feature_importance(booster: Booster) -> pd.DataFrame:
    """
    Generates a feature importance table containing Gain and Split

    :param booster: trained LGM model

    :return: feature importance table
    """

    feats = list(
        zip(
            booster.feature_name(),
            booster.feature_importance(importance_type="gain"),
            booster.feature_importance(importance_type="split"),
        )
    )

    feats = pd.DataFrame(
        feats, columns=["feature", "importance_gain", "importance_split"]
    )

    feats["importance_gain"] = feats["importance_gain"] / feats["importance_gain"].sum()
    feats["importance_split"] = (
        feats["importance_split"] / feats["importance_split"].sum()
    )
    feats = feats.sort_values("importance_gain", ascending=False)

    return feats.reset_index(drop=True)


def one_hot_encoding(
    df: pd.DataFrame,
    str_col: str,
    labels: List[str] = None,
    drop: bool = True,
    add_other: bool = True,
) -> pd.DataFrame:
    """
    Generates one-hot-encoding variables

    :param df: table to modify (type: pd.DataFrame)
    :param str_col: name of column to one-hot-encode (type: str)
    :param labels: possible labels (type: str)
    :param drop: flag to drop str_col after computing one-hot-encoding (type: bool)
    :param add_other: flag to add an "other" column for those values not
    in labels (type: bool)

    :return: modified table (type: pd.DataFrame)
    """

    if labels is None:
        print("[ONE-HOT-ENCODING] No label list especified, inferring from data")
        labels = df[str_col].unique()
    print("[ONE-HOT-ENCODING] Labels are: {labels}")

    print("[ONE-HOT-ENCODING] Generating new variables")
    for label in labels:
        df[f"{str_col}_{label}"] = np.where(df[str_col] == label, 1, 0)

    if add_other:
        print("[ONE-HOT-ENCODING] Including variable for missing label")
        df[f"{str_col}_OTHER"] = np.where(~df[str_col].isin(labels), 1, 0)

    if drop:
        print("[ONE-HOT-ENCODING] Dropping original categorical column")
        df.drop(columns=str_col, axis=1, inplace=True)

    return df


def build_lgm_datasets(
    x_train: np.array,
    y_train: np.array,
    x_dev: np.array,
    y_dev: np.array,
    train_columns: List[str],
    do_log: bool,
) -> Tuple[Dataset, Dataset]:
    """
    Given train/dev splitted data, generate LightGBM dataset objects for model train

    :param x_train: train data
    :param y_train: train target
    :param x_dev: dev data
    :param y_dev: dev features
    :param train_columns: name of feature columns in master table
    :param do_log: flag to apply log transformation

    :return: LightGBM datasets
    """

    if do_log:
        train_dataset = Dataset(x_train, to_log(y_train), feature_name=train_columns)
        dev_dataset = Dataset(x_dev, to_log(y_dev), feature_name=train_columns)

    else:
        train_dataset = Dataset(x_train, y_train, feature_name=train_columns)
        dev_dataset = Dataset(x_dev, y_dev, feature_name=train_columns)

    return train_dataset, dev_dataset


def train_dev_split_oot(
    master: pd.DataFrame,
    train_columns: List[str],
    target_column: str,
    date_train_until: pd.Timestamp,
    num_days_train: int,
    num_days_dev: int,
    date_column: str = "date",
) -> Tuple[np.array, np.array, np.array, np.array]:
    """
    Splits master into Train and Dev in out-of-time

    :param master: master table
    :param train_columns: list of columns to use in tain
    :param target_column: name of target column
    :param date_train_until: maximum date to use
    :param num_days_train: number of days to use for train
    :param num_days_dev:  number of days to use for Dev
    :param date_column: name of date column

    :return: train-dev X and y
    """

    date_train_from = date_train_until - pd.Timedelta(
        num_days_train + num_days_dev, unit="D"
    )
    date_train_to = date_train_until - pd.Timedelta(num_days_dev, unit="D")
    date_dev_from = date_train_until - pd.Timedelta(num_days_dev, unit="D")
    date_dev_to = date_train_until

    print("Date configuration:")
    print(
        "Train: {} / {}   |  dev: {} / {}".format(
            date_train_from, date_train_to, date_dev_from, date_dev_to,
        )
    )
    print("Date configuration:")

    print("Train-dev split")
    master_train = master[
        (master[date_column] >= pd.Timestamp(date_train_from))
        & (master[date_column] < pd.Timestamp(date_train_to))
        & (~master[target_column].isna())
    ].copy()

    master_dev = master[
        (master[date_column] >= pd.Timestamp(date_dev_from))
        & (master[date_column] < pd.Timestamp(date_dev_to))
        & (~master[target_column].isna())
    ].copy()

    x_train = master_train[train_columns].values
    y_train = master_train[target_column].values
    x_dev = master_dev[train_columns].values
    y_dev = master_dev[target_column].values

    return x_train, y_train, x_dev, y_dev
