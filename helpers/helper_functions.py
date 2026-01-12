"""
Helper functions used accross process
"""

import logging

from datetime import date

import pyodbc

import pandas as pd

from helpers.process_constants import PROCESS_CONSTANTS

logger = logging.getLogger(__name__)


def combine_with_af_email(item_df: pd.DataFrame):
    """Combines items with AF emails from LIS database"""

    connection_string_mbu = PROCESS_CONSTANTS["DBCONNECTIONSTRINGPROD"]

    af_email = af_losid(connection_str=connection_string_mbu)
    af_email_df = pd.DataFrame(af_email).astype({"LOSID": int}, errors="ignore")
    combined_df = pd.merge(left=item_df, right=af_email_df, on="LOSID")

    lis_dep = lis_enheder(connection_string=connection_string_mbu)
    lis_dep_df = (
        pd.DataFrame(lis_dep)
        .rename(columns={"losid": "LOSID"})
        .astype({"LOSID": int}, errors="ignore")
    )

    combined_df = pd.merge(left=combined_df, right=lis_dep_df, on="LOSID")

    items = list(combined_df.T.to_dict().values())

    return items


def format_item(item: dict):
    """Format dates in dict, e.g. for json parsing"""

    return {
        key: value.strftime("%d-%m-%Y") if isinstance(value, date) else value
        for key, value in item.items()
    }


# def find_match_ovk(ovk: str):
#     """To find matching overenskomst, maybe?"""
#     # Some lookup
#     match_ovk = ""

#     return match_ovk


def find_pair_info(data: dict, number: int):
    """
    Searches for `number` (int) in `data['pair']`. If found, returns the other element from the pair
    and the corresponding name from `pair_names`.

    Args:
        data (dict): Dictionary with keys:
                     - 'ovk' (str)
                     - 'pair' (tuple of two elements)
                     - 'pair_names' (tuple of two elements)
        number (int): Number to search for in the pair

    Returns:
        tuple: (other_value, corresponding_name) if found, else None
    """

    pair = data.get('pair')
    pair_names = data.get('pair_names')

    if isinstance(pair, tuple) and isinstance(pair_names, tuple) and len(pair) == 2 and len(pair_names) == 2:
        if number in pair:
            index = pair.index(number)
            other_index = 1 - index

            return pair[other_index], pair_names[other_index]

    return None


def get_items_from_query(connection_string, query: str):
    """Executes given sql query and returns rows from its SELECT statement"""

    result = []
    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:

                cursor.execute(query)

                rows = cursor.fetchall()

                # Get column names from cursor description
                columns = [column[0] for column in cursor.description]

                # Convert to list of dictionaries
                result = [
                    {
                        column: value.strip() if isinstance(value, str) else value
                        for column, value in zip(columns, row)
                    }
                    for row in rows
                ]

    except pyodbc.Error as e:
        logger.info(f"Database error: {str(e)}")
        logger.info(f"{connection_string}")

        raise e

    except ValueError as e:
        logger.info(f"Value error: {str(e)}")

        raise e

    # pylint: disable-next = broad-exception-caught
    except Exception as e:
        logger.info(f"An unexpected error occurred: {str(e)}")

        raise e

    if len(result) == 0:
        return None

    return result


def get_items_from_query_with_params(
    connection_string,
    query: str,
    params: list | tuple
):
    """
    Executes a parameterized SQL query and returns rows as list of dicts.
    """

    result = []

    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:

                cursor.execute(query, params)

                rows = cursor.fetchall()

                columns = [column[0] for column in cursor.description]

                result = [
                    {
                        column: value.strip() if isinstance(value, str) else value
                        for column, value in zip(columns, row)
                    }
                    for row in rows
                ]

    except pyodbc.Error as e:
        logger.info(f"Database error: {str(e)}")
        logger.info(f"{connection_string}")
        raise

    except Exception as e:
        logger.info(f"Unexpected error: {str(e)}")
        raise

    return result or None


def lis_enheder(connection_string: str, afdtype: tuple | None = None):
    """Get the right departments from LIS stamdata"""

    sql = """
        SELECT
            distinct lisid,
            losid,
            enhnavn,
            afdtype,
            afdtype_txt
        FROM
            [BuMasterdata].[dbo].[VIEW_MD_STAMDATA_AKTUEL]
    """

    if afdtype:
        sql += (
            f"""
                WHERE
                    afdtype IN {afdtype}
            """
        )

    departments = get_items_from_query(connection_string=connection_string, query=sql)

    return departments


def sd_enheder(connection_string: str, losid_tuple: tuple | None = None):
    """Get SDafdID from faellessql"""

    sql = """
        SELECT
            SDafdID,
            LOSID
        FROM
            [Personale].[sd].[Organisation]
    """

    if losid_tuple:
        sql += (
            f"""
                WHERE
                    LOSID IN {losid_tuple}
            """
        )

    departments = get_items_from_query(connection_string=connection_string, query=sql)

    return departments


def af_losid(connection_str: str):
    """Get AF per LOSID"""

    sql = """
    SELECT
        v1.afdemail AS AF_email,
        v2.LOSID
    FROM
        (
        SELECT
            adm_faelles_id,
            lisid
        FROM
            [BuMasterdata].[dbo].[MD_ADM_FAELLESSKAB]
        WHERE
            STARTDATO <= GETDATE()
            and SLUTDATO > GETDATE()
        ) t
    LEFT JOIN
        [BuMasterdata].[dbo].[VIEW_MD_STAMDATA_AKTUEL] v1 ON t.adm_faelles_id = v1.lisid
    LEFT JOIN
        [BuMasterdata].[dbo].[VIEW_MD_STAMDATA_AKTUEL] v2 ON t.lisid = v2.lisid
    """

    af_email_kobling = get_items_from_query(connection_string=connection_str, query=sql)

    return af_email_kobling


def build_tillaeg_pairs_cte(pairs: list[dict]) -> str:
    rows = []
    for p in pairs:
        ovk = p["ovk"]
        a, b = p["pair"]
        rows.append(f"({ovk}, {a}, {b})")
    return ",\n".join(rows)
