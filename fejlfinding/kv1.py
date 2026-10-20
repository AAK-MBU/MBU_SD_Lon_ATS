"""KV1: finder inspirationsansættelser oprettet på XA-enheder"""

import pandas as pd

from helpers import proces_helpers
from helpers.process_constants import PROCESS_CONSTANTS


def kv1(overenskomst: int):
    """
    CASE: ANSAT PÅ OVERENSKOMST 47302 OG INSTITUTIONSKODE IKKE XC

    Arguments:
        overenskomst (int): The "overenskomst" to look for
        connection_string (string): Connection string for pyodbc connection

    Returns:
        items (list | None): List of items from the SELECT query. If no elements fits the query then returns None
    """

    sql = f"""
        SELECT
            ans.Tjenestenummer,
            ans.Overenskomst,
            ans.Afdeling,
            ans.Institutionskode,
            perstam.Navn,
            ans.Startdato,
            ans.Slutdato,
            ans.Statuskode,
            org.LOSID
        FROM
            [Personale].[sd_magistrat].[Ansættelse_mbu] ans
        RIGHT JOIN
            [Personale].[sd].[personStam] perstam ON ans.CPR = perstam.CPR
        LEFT JOIN
            [Personale].[sd].[Organisation] org ON ans.Afdeling = org.SDafdID
        WHERE
            Slutdato > getdate() and Startdato <= getdate()
            and ans.Overenskomst={overenskomst}
            and ans.Statuskode in ('1', '3', '5')
            and ans.Institutionskode!='XC'
    """

    proc_args = PROCESS_CONSTANTS["kv_proc_args"]
    receiver = proc_args.get("notification_receiver", None).upper()
    af_receiver = receiver == "AF"

    connection_string = PROCESS_CONSTANTS["FaellesDbConnectionString"]

    items = proces_helpers.get_items_from_query(connection_string, sql)

    if items and af_receiver:
        item_df = pd.DataFrame(items).astype({"LOSID": int}, errors="ignore")

        items = proces_helpers.combine_with_af_email(item_df=item_df)

    return items
