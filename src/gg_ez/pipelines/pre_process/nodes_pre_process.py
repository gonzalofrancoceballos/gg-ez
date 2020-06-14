import pandas as pd

from gg_ez.utilities.io import JSONData
from gg_ez.utilities.processing import apply_multiprocessing
from gg_ez.pre_process.pre_process_fixture import process_fixture


def process_fixtures_leagues(
    fixture_league: JSONData, n_cores: int = 1
) -> pd.DataFrame:
    """

    :param fixture_league:
    :param n_cores:

    :return:
    """

    fixture_league_data = fixture_league.get_data()
    leagues = list(fixture_league_data.values())
    processed_fixtures = apply_multiprocessing(wrapper, leagues, n_cores=n_cores)

    processed_fixtures = pd.concat(processed_fixtures)
    return processed_fixtures


def wrapper(league):
    res = []
    for fixture in league["api"]["fixtures"]:
        res.append(process_fixture(fixture))
    return pd.concat(res)
