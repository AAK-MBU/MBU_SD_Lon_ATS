"""KV6: finder ændringer i lønsammensætning for medarbejdere på gammel fleksordning"""

import pandas as pd

from helpers import ats_helpers, proces_helpers
from helpers.process_constants import PROCESS_CONSTANTS


def kv6():
    """
    Get monthly wages for current and previous month for a group of employees, and check that wages haven't changed.
    """

    conn_str_mbu = PROCESS_CONSTANTS["DBCONNECTIONSTRINGPROD"]

    tjenestenumre_df = proces_helpers.get_tjenestenumre(conn_str_mbu)

    tjenestenumre = tuple(tjenestenumre_df["Tjenestenummer"].values)

    wage_data = proces_helpers.compare_wages_month_prior(tjenestenumre=tjenestenumre)

    wage_df = pd.DataFrame(wage_data)

    wage_df = wage_df.rename(
        columns={
            "Sum_Beløb": "sum_tillægsbeløb_nu",
            "Sum_Beløb_Prev": "sum_tillægsbeløb_forrige",
            "Sum_Trin": "sum_trin_nu",
            "Sum_Trin_Prev": "sum_trin_forrige",
            "Delta_Beløb": "ændringer_beløb",
            "Delta_Trin": "ændringer_trin",
        }
    )

    items_df = wage_df[
        ((wage_df["ændringer_beløb"] != 0) | (wage_df["ændringer_trin"] != 0))
    ]

    items_df = items_df.merge(
        tjenestenumre_df[["Tjenestenummer", "Personale_gruppe", "Ansaettelsestype"]],
        on="Tjenestenummer",
        how="left",
    )

    items = ats_helpers.item_df_to_item_list(item_df=items_df)

    return items
