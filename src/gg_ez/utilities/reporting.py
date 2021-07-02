from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Union

import numpy as np
import pandas as pd
from lightgbm import Booster

from utilities.io import save_yaml
from utilities.model import get_feature_importance
from utilities.plot import plot_prediction_distributions, plot_prediction_vs_target


def save_reports_regression(
    model_path: Path,
    model: Booster,
    y: Tuple[np.array, np.array, np.array, np.array],
    x_dev: np.array,
    eval_results: dict,
    train_columns: List[str],
    config_regression: dict,
    target_column: str = "target",
):
    """

    :param model_path:
    :param model:
    :param y:
    :param x_dev:
    :param eval_results:
    :param train_columns:
    :param config_regression:
    :param target_column:

    :return:
    """

    variables = {"train_columns": train_columns, "target_column": target_column}

    save_yaml(config_regression, model_path / "config_regression.yml")
    save_yaml(variables, model_path / "variables.yml")
    model.save_model(str(model_path / "model_regression.lgm"))
    y_train, y_train_pred, y_dev, y_dev_pred = y
    train_columns = model.feature_name()

    feature_importance = get_feature_importance(model)
    feature_importance.to_excel(model_path / "feature_importance.xlsx", index=False)

    train_log = process_train_log(eval_results)
    train_log.to_excel(model_path / "train_log .xlsx", index=False)

    plot_prediction_vs_target(
        y_train,
        y_train_pred,
        y_dev,
        y_dev_pred,
        img_path=model_path / "target_vs_pred.png",
    )

    plot_prediction_distributions(
        y_train,
        y_train_pred,
        y_dev,
        y_dev_pred,
        img_path=model_path / "distributions.png",
    )

    predictions_df = pd.DataFrame(x_dev, columns=train_columns)
    predictions_df[target_column] = y_dev
    predictions_df["pred"] = y_dev_pred

    var_avg, var_nan = variable_summary_by_percentile(
        predictions_df,
        columns_to_score=train_columns + [target_column],
        score_col="pred",
        n_bins=10,
        feature_importance=feature_importance,
    )

    var_avg.to_excel(model_path / "decile_means.xlsx", index=False)
    var_nan.to_excel(model_path / "decile_nas.xlsx", index=False)


def save_reports_quantile(
    model_path: Union[str, Path],
    models: Dict[float, Booster],
    train_logs: Dict[Any, Any],
    train_columns: Iterable[str],
    target_column: str,
):
    """

    :param model_path:
    :param models:
    :param train_logs:
    :param train_columns:
    :param target_column:

    :return:
    """

    variables = {"train_columns": train_columns, "target_column": target_column}

    save_yaml(variables, model_path / "variables.yml")
    for q in models.keys():
        k = int(100 * q)
        models[q].save_model(str(model_path / f"model_quantile_{k}.lgm"))
        feature_importance = get_feature_importance(models[q])
        feature_importance.to_excel(
            model_path / f"feature_importance_{k}.xlsx", index=False
        )

    for q in train_logs.keys():
        k = int(100 * q)
        train_log = process_train_log(train_logs[q])
        train_log.to_excel(model_path / f"train_log_{k}.xlsx", index=False)


def variable_avg_by_percentile(
    table: pd.DataFrame,
    feature_vars: Iterable[str],
    percentile_col: str = "percentile",
    feature_importance: pd.DataFrame = pd.DataFrame(),
) -> pd.DataFrame:
    """
    Compute mean of vars in a master table by decile of score
    :param table: master table (type: pd.DataFrame)
    :param feature_vars: (type: list)
    :param percentile_col: column name that stores the column to groupby (type: str)
    :param feature_importance: feature importance table (type: pd.DataFrame)

    :return: pd.DataFrame with the result
    """

    result = (
        table.groupby([percentile_col])[feature_vars]
        .mean()
        .transpose()
        .reset_index()
        .rename(columns={"index": "feature"})
    )

    if not feature_importance.empty:
        result = result.merge(feature_importance, on="feature", how="left")
        result = result.sort_values("importance_gain", ascending=False)

    return result


def variable_na_by_percentile(
    table: pd.DataFrame,
    feature_vars: Iterable[str],
    percentile_col: str = "percentile",
    feature_importance: pd.DataFrame = pd.DataFrame(),
) -> pd.DataFrame:
    """
    Compute % of NAs in a master table by decile of score

    :param table: master table (type: pd.DataFrame)
    :param feature_vars: (type: list)
    :param percentile_col: column name that stores the column to groupby (type: str)
    :param feature_importance: feature importance table (type: pd.DataFrame)

    :return: pd.DataFrame with the result
    """

    result = (
        table.groupby([percentile_col])[feature_vars]
        .apply(na_mean)
        .transpose()
        .reset_index()
        .rename(columns={"index": "feature"})
    )

    if not feature_importance.empty:
        result = result.merge(feature_importance, on="feature", how="left")
        result = result.sort_values("importance_gain", ascending=False)

    return result


def na_mean(x: np.array) -> np.array:
    """
    Computes the percentaje of NAs in a np.array

    :param x: array to compute operation on

    :return: % of NAs in array
    """

    return np.mean(np.isnan(x))


def variable_summary_by_percentile(
    predictions_df: pd.DataFrame,
    columns_to_score: List[str],
    score_col: str = "score",
    n_bins: int = 10,
    feature_importance: pd.DataFrame = pd.DataFrame(),
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Computes the summary variable in predictions_df (selecting only
    columns_to_score), splitted by percentile

    :param predictions_df: pd.DataFrame with master tale and predictions
        (type: pd.DataFrame)
    :param columns_to_score: List with the names of the variables in predictions_df
        (type: list)
    :param score_col: name of score column (type: str)
    :param n_bins: number of bins. 10=deciles, 100=percentiles, etc (type: int)
    :param feature_importance: feature importance table (type: pd.DataFrame)

    :returns: summaries
    """

    # Filter and sort
    predictions_df = predictions_df[columns_to_score + [score_col]]
    predictions_df = predictions_df.sort_values(score_col, ascending=True)
    row_count = predictions_df.shape[0]
    predictions_df["percentile"] = (
        np.floor((1 - np.arange(1, row_count + 1) / row_count) * n_bins) + 1
    )

    var_avg = variable_avg_by_percentile(
        table=predictions_df,
        feature_vars=columns_to_score,
        feature_importance=feature_importance,
    )
    var_nan = variable_na_by_percentile(
        table=predictions_df,
        feature_vars=columns_to_score,
        feature_importance=feature_importance,
    )

    return var_avg, var_nan


def process_train_log(eval_results: dict):
    """

    :param eval_results:

    :return:
    """

    train_log = []
    for dataset in eval_results.keys():
        eval_i = pd.DataFrame(eval_results[dataset])
        eval_i.columns = [f"{dataset}_{col}" for col in eval_i.columns]
        train_log.append(eval_i)
    train_log = pd.concat(train_log, axis=1)

    return train_log
