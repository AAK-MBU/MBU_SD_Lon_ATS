"""KV4: finder ledere uden låst anciennitetsdato"""

import pandas as pd

from helpers import proces_helpers
from helpers.process_constants import PROCESS_CONSTANTS


def kv4(leder_overenskomst: tuple):
    """
    CASE: Ledere som mangler lås på anciennitetsdato.
    """

    sql = f"""
        SELECT
            ans.Tjenestenummer, ans.Overenskomst, ans.Afdeling, perstam.Navn, ans.Institutionskode,
            ans.Anciennitetsdato, org.LOSID
        FROM
            [Personale].[sd_magistrat].Ansættelse_mbu ans
            right join [Personale].[sd].[personStam] perstam
                on ans.CPR = perstam.CPR
            left join [Personale].[sd].[Organisation] org
                on ans.Afdeling = org.SDafdID
        WHERE
            ans.Overenskomst in {leder_overenskomst}
            and ans.Startdato <= GETDATE() and ans.Slutdato > GETDATE() and ans.Statuskode in ('1', '3', '5')
            and cast(ans.Anciennitetsdato as date) != '9999-12-31'
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
