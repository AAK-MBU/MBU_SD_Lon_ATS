import pandas as pd

from helpers import helper_functions


def get_tjenestenumre(conn_str: str) -> pd.DataFrame:
    sql = """
        SELECT
            *
        FROM
            [RPA].[sdlon].[gl_fleksordning]
    """
    items = helper_functions.get_items_from_query(conn_str, sql)
    items_df = pd.DataFrame(items)

    return items_df
