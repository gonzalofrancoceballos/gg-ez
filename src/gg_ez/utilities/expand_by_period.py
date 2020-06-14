from multiprocessing.dummy import Pool
from typing import Callable, Union

import numpy as np
import pandas as pd

from utilities.decorators import rename


def expand_by_period(
    df_raw: pd.DataFrame,
    id_col: list,
    date_col: list,
    periods: list,
    agg: Union[str, Callable, list, dict],
    suffix: str = "H",
    columns_to_expand: list = None,
    latest_period_available: int = 0,
    return_only_agg_columns: bool = False,
    n_jobs: int = 1,
    copy: bool = False,
    verbose: bool = False,
):
    """

    Efficient implementation of a process for creating time-wise aggregations
    for a set of variables for each ID

        - The sampling frequency between periods must be constant

        - The date variable(s) must be sortable

        - Different IDs can have distinct date ranges

    The resulting dataframe will contain variables named with the following
    pattern

        '{agg_column_name}_{agg_name}_{period}{suffix}'

        ex: 'SALARY_mean_1W'

    :param df_raw: dataframe to be expanded (pd.DataFrame)

    :param id_col: name of the column(s) to use as id (list)

    :param date_col: name of the column(s) to use as time period (list).

    :param periods: number of periods to aggregate. Positive periods mean
    expanding towards the past while negative

    ones mean expansions towards the future. E.g. periods = [1, 2, 3] means that
    the previous (1), (1,2) and

    (1,2,3) periods will be aggregated together for producing 3 new columns
    (list)

    :param suffix: suffix to be added to new column names (str)

    :param columns_to_expand: list of columns to process. If not defined, all
    columns will be used.

        If agg is given a dictionary, this parameter is omitted (list)

    :param agg: str, function, list of strs and functions, or dict. If a dict is
    given,

        columns_to_expand parameter is ignored. Otherwise the defined aggregation(s) will

        be applied to specified columns. Strings are assumed to be numpy functions (i.e.

        given str 'mean', it is assumed to be np.mean) (str, callable, list or dict)

    :param latest_period_available: how many periods of lag until date are
    available (Default = 0)

        (int).

    :param return_only_agg_columns: If `True`, returns only newly created
    columns as a DF. (Default

        = False) (bool)

    :param n_jobs: number of parallel jobs to use to run the process (Default =
    1) (int)

    :param copy: if set to True, a copy of the df_raw is made in order to avoid
    modifying the original data

    by reference (pd.DataFrame)

    :param verbose: (bool)

    :returns: DataFrame with new aggregated columns, left joined to the original
    DataFrame. The order

        of the items in the returned df will be sorted by id asc and date_col desc

        (i.e. the latest observation will be the first) (pd.DataFrame)

    Example:

        df = expand_by_period_weekly(df, periods=[1, 4, 6, 10], agg={
            'SALARY': [np.mean, 'min', 'max'],
            'SALARY_CNT': 'mean'})
    """
    if not isinstance(agg, dict):
        # Variable agg will be a dict with {'colname':[aggs]}

        # If not specified, take all the columns.
        if columns_to_expand is None:
            columns_to_expand = [
                c for c in df_raw.columns if c not in [id_col] + date_col
            ]

        if isinstance(columns_to_expand, str):
            columns_to_expand = [columns_to_expand]

        if not isinstance(columns_to_expand, list):
            raise ValueError(
                "Parameter columns_to_expand should be None, str or list of strings"
            )

        # We convert agg to a dictionary for easier processing later on.
        agg = {col: agg for col in columns_to_expand}

    # first get a deep copy to avoid overwriting
    df = df_raw.copy() if copy else df_raw

    if verbose:
        print("Started expand_by_period.")

    # First sort the df. (about 1 min for a big df)
    df = df.sort_values(
        by=id_col + date_col,
        ascending=[True for _ in id_col] + [False for _ in date_col],
    )
    if verbose:
        print("Sorted df.")

    # Create a Mask with True values where the IDs change
    mask = pd.MultiIndex.from_arrays([df[x].values for x in id_col])
    mask = mask[:-1] != mask[1:]

    if verbose:
        print("Created the mask for rolling synthetic variable.")

    if n_jobs == 1:
        new_features = list(
            map(
                lambda agg_col, agg_funcs: _generate_feature_aggregations(
                    df=df,
                    agg_col=agg_col,
                    agg_funcs=agg_funcs,
                    mask=mask,
                    periods=periods,
                    suffix=suffix,
                    latest_period_available=latest_period_available,
                    verbose=verbose,
                ),
                list(agg.keys()),
                list(agg.values()),
            )
        )
    else:
        pool = Pool(n_jobs)
        new_features = pool.map(
            lambda x: _generate_feature_aggregations(
                df=df,
                agg_col=x[0],
                agg_funcs=x[1],
                mask=mask,
                periods=periods,
                suffix=suffix,
                latest_period_available=latest_period_available,
                verbose=verbose,
            ),
            list(agg.items()),
        )
        pool.close()
        pool.join()

    new_features = [feature for sublist in new_features for feature in sublist]

    # tmp is the DataFrame we will create new columns on.
    df_tmp = df.loc[:, id_col + date_col] if return_only_agg_columns else df
    while len(new_features):
        name, value = new_features.pop(0)
        df_tmp[name] = value

    return df_tmp


@rename("dp")
def datapoint(x, axis):
    """
    Aggregation function to be used in expand_by_period in order to select the
    single datapoint and not aggregate with
    other columns
    :param x: shift matrix (np.array)
    :param axis: dummy argument for compatibility with the expand_by_period
    :return: 0th element of the shift matrix, at axis 0 (np.array)
    """
    return x[0, :]


def _generate_feature_aggregations(
    df: pd.DataFrame,
    agg_col: str,
    agg_funcs: Union[list, str],
    mask: np.array,
    periods: list,
    suffix: str,
    latest_period_available: int,
    verbose: bool,
):
    """
    Given a DataFrame, this function applies the aggregation function specified
    over the periods indicated.
    :param df: dataframe to use as a base for generating the aggregations
    :param agg_col: name of the column to aggregate by (str)
    :param agg_funcs: functions to do the aggregations (list or str)
    :param mask: vector indicating where the primary key changes (np.array)
    :param periods: periods to consider in the aggregations (list)
    :param suffix: suffix to add at the end of the variables. Those have the
    following structure
    "{agg_col}_{agg_name}_{period}{suffix}"
    :param latest_period_available: how many periods of lag until data is
    available (Default = 0) (str)
    :param verbose: indicates if the function must print messages (bool)
    :return: list of tuples, each of them indicating the name of the feature
    created and the feature itself (list)
    """
    feat = df[agg_col].values
    aggregations = []
    shift_matrix = _generate_shift_matrix(feature=feat, mask=mask, periods=periods)

    # Lets do the aggregations! =)
    agg_funcs = [agg_funcs] if not isinstance(agg_funcs, list) else agg_funcs

    if verbose:
        print("Built shifted value matrix for column:", agg_col)

    # Do one aggregation at a time!
    for agg_func in agg_funcs:

        # Get agg name string, if supplied a function like np.mean etc.
        agg_name = agg_func if isinstance(agg_func, str) else agg_func.__name__

        # Do one aggregation for a period at a time!
        for period in periods:

            new_column_name = f"{agg_col}_{agg_name}_{period}{suffix}"

            if not callable(agg_func):
                # it is a string, so assume it is np.xxx function.
                agg_func = getattr(np, agg_func)
            if period > 0:  # Past
                aggregation = agg_func(
                    shift_matrix[
                        (
                            np.abs(np.min([min(periods), 0])) + latest_period_available
                        ) : (  # noqa: E203
                            np.abs(np.min([min(periods), 0])) + period
                        ),
                        :,
                    ][::-1],
                    axis=0,
                )
                # Axis inversion to have the most close to the present time in the first position
            else:  # Future
                aggregation = agg_func(
                    shift_matrix[
                        (period - min(periods)) : np.abs(min(periods)), :  # noqa: E203
                    ],
                    axis=0,
                )
            aggregations.append((new_column_name, aggregation))

            if verbose:
                print("Created feature:", new_column_name)

    return aggregations


def _generate_shift_matrix(feature: np.array, mask: np.array, periods: list):
    """
    Generates a matrix with the shift periods needed for building the expanded
    dataframe. This function is called from
    the expand_by_period function.
    :param feature: vector with the feature data, extracted from the sorted
    dataframe (np.array)
    :param mask: vector with the same elements as 'feature' - 1 with boolean
    values, indicating if the primary key has
    changed (np.array)
    :param periods: periods to shift by (list)
    :return: matrix with as many rows as the original data has, and (max_period
    - min_period + 1) columns

    # Example
    master = expand_by_period(
        master,
        date_col=["date", "hour", "minute"],
        id_col=["key_col_1", "key_col_2],
        suffix="_15min",
        columns_to_expand=cols_to_expand_15min,
        periods=[1, 4],
        agg=[np.nanmean, np.nanmax, np.nanmin],
    )
    """

    # We'll convert int types to float, to be able to place np.nan in some cells.
    if np.issubdtype(feature.dtype, np.integer):
        feature = feature.astype(np.float)

    feature_original = feature.copy()

    # We'll collect shifted arrays in this variable.
    shift_matrix = [feature]

    # And we shift the data one-by-one, omit the value at id change, then collect in shift_matrix.
    for period in range(np.min([min(periods), 0]), np.max([max(periods), 0])):
        if period == 0:
            feature = feature_original.copy()
        if period >= 0:
            feature_new = feature[1:].copy()
        else:
            feature_new = feature[:-1].copy()
        feature_new[mask] = np.nan  # Apply the mask
        if period >= 0:
            feature_new = np.hstack((feature_new, [np.nan]))
            shift_matrix.append(feature_new)
        else:
            feature_new = np.hstack(([np.nan], feature_new))
            shift_matrix.insert(0, feature_new)
        feature = feature_new
        del feature_new
    return np.array(shift_matrix)
