"""KV3: finder ansættelser med forkert overenskomst i skole/dagtilbud"""

import logging

import pandas as pd

from helpers import proces_helpers
from helpers.process_constants import PROCESS_CONSTANTS

logger = logging.getLogger(__name__)


def kv3(
    accept_ovk_dag: tuple,
    accept_ovk_skole: tuple,
):
    """Ansættelser with wrong overenskomst based on departmentype"""

    connection_string_mbu = PROCESS_CONSTANTS["DBCONNECTIONSTRINGPROD"]
    connection_string_faelles = PROCESS_CONSTANTS["FaellesDbConnectionString"]

    # Load department types from LIS stamdata
    lis_stamdata = proces_helpers.lis_enheder(
        connection_string=connection_string_mbu, afdtype=(2, 3, 4, 5, 11, 13)
    )
    losid_tuple = tuple(i["losid"] for i in lis_stamdata)

    # Load corresponding SD department codes
    sd_departments = proces_helpers.sd_enheder(
        losid_tuple=losid_tuple, connection_string=connection_string_faelles
    )

    # Combine SD and LIS data
    lis_stamdata_df = pd.DataFrame(lis_stamdata).rename(columns={"losid": "LOSID"})
    lis_stamdata_df["LOSID"] = lis_stamdata_df["LOSID"].astype(int)

    sd_departments_df = pd.DataFrame(sd_departments)
    sd_departments_df["LOSID"] = sd_departments_df["LOSID"].astype(int)

    combined_df = pd.merge(
        left=lis_stamdata_df, right=sd_departments_df, how="outer", on="LOSID"
    )

    # Filter dagtilbud and skole respectively
    dagtilbud_df = combined_df[
        (
            (combined_df["afdtype"].isin([2, 3, 4, 5, 11]))
            & (combined_df["SDafdID"].notna())
        )
    ]
    dagtilbud_afd = tuple(dagtilbud_df["SDafdID"].values)

    skole_df = combined_df[
        ((combined_df["afdtype"].isin([13])) & ~(combined_df["SDafdID"].isna()))
    ]
    skole_afd = tuple(skole_df["SDafdID"].values)

    # Collect ansættelser with wrong overenskomst
    items = kv3_1(
        connection_str=connection_string_faelles,
        skole_afd=skole_afd,
        dagtilbud_afd=dagtilbud_afd,
        accept_ovk_skole=accept_ovk_skole,
        accept_ovk_dag=accept_ovk_dag,
    )
    items_df = pd.DataFrame(items)

    if items_df.empty:
        logger.info("KV3: No wrong overenskomster found. Returning empty result.")

        return []

    # Combine with other information
    combined_df = pd.merge(
        left=combined_df, right=items_df, left_on="SDafdID", right_on="Afdeling"
    )
    combined_df["Startdato"] = combined_df["Startdato"].astype(str)
    combined_df["Slutdato"] = combined_df["Slutdato"].astype(str)
    combined_df = combined_df.rename(columns={"enhnavn": "Enhedsnavn"})[
        [
            "Tjenestenummer",
            "Afdeling",
            "Institutionskode",
            "Overenskomst",
            "Enhedsnavn",
            "Navn",
            "afdtype_txt",
        ]
    ]

    # Format data as list of dicts. Each list element is a row in the dataframe
    items = list(combined_df.T.to_dict().values())

    return items


def kv3_1(
    connection_str: str,
    skole_afd: tuple,
    dagtilbud_afd: tuple,
    accept_ovk_skole: tuple,
    accept_ovk_dag: tuple,
):
    """Get wrong overenskomst in skole and dagtilbud respectively"""
    accept_dag_str = (
        f"AND Overenskomst NOT IN {accept_ovk_dag}" if len(accept_ovk_dag) != 0 else ""
    )
    accept_skole_str = (
        f"AND Overenskomst NOT IN {accept_ovk_skole}"
        if len(accept_ovk_skole) != 0
        else ""
    )
    sql = f"""
        SELECT
            ans.Tjenestenummer,
            ans.Overenskomst,
            ans.Afdeling,
            ans.Institutionskode,
            perstam.Navn,
            ans.Startdato,
            ans.Slutdato,
            ans.Statuskode
        FROM
            [Personale].[sd_magistrat].[Ansættelse_mbu] ans
        LEFT JOIN
            [Personale].[sd].[personStam] AS perstam ON ans.CPR = perstam.CPR
        WHERE
            (
                (
                    Afdeling IN {dagtilbud_afd}
                    AND Overenskomst IN (76001, 76101, 77001)
                    {accept_dag_str}
                )
                OR
                (
                    Afdeling IN {skole_afd}
                    AND Overenskomst IN (46001, 46101)
                    {accept_skole_str}
                )
            )
            AND Statuskode in ('1', '3', '5')
            AND Startdato <= GETDATE()
            AND Slutdato > GETDATE()
    """

    logger.info(f"printing the full sql:\n\n{sql}\n\n")

    items = proces_helpers.get_items_from_query(
        connection_string=connection_str, query=sql
    )

    return items
