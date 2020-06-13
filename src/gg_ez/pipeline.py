"""Construction of the master pipeline.
"""

from typing import Dict

from kedro.pipeline import Pipeline
from gg_ez.pipelines import fetch

###########################################################################
# Here you can find an example pipeline, made of two modular pipelines.
#
# Delete this when you start working on your own Kedro project as
# well as pipelines/data_science AND pipelines/data_engineering
# -------------------------------------------------------------------------


def create_pipelines(**kwargs) -> Dict[str, Pipeline]:
    """Create the project's pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.

    """

    fetch_stats_pipeline = fetch.create_pipeline()

    return {
        "fetch": fetch_stats_pipeline,
        "__default__": fetch_stats_pipeline,
    }
