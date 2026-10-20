"""KV2: finder ansættelser der mangler det andet tillægsnummer i et tillægspar"""

import logging

import pandas as pd

from helpers import proces_helpers
from helpers.process_constants import PROCESS_CONSTANTS

logger = logging.getLogger(__name__)


def kv2(tillaegsnr_par: list):
    """
    CASE: HAS ONLY ONE OF A PAIR OF 'TILLÆGSNUMRE'

    Arguments:
        tillaegsnr_par (list): List of dicts with keys:
            - 'ovk' (int)
            - 'pair' (tuple of two elements)
            - 'pair_names' (tuple of two elements)

    Returns:
        items (list): List of items from the SELECT query.
                      Returns empty list if no issues found.
    """

    connection_string_faelles_sql = PROCESS_CONSTANTS["FaellesDbConnectionString"]
    connection_string_mbu = PROCESS_CONSTANTS["DBCONNECTIONSTRINGPROD"]

    # Build VALUES table for all A/B pairs
    pairs_values = proces_helpers.build_tillaeg_pairs_cte(tillaegsnr_par)

    sql = f"""
        WITH TillægPairs (Overenskomst, TillægA, TillægB) AS (
            SELECT
                *
            FROM
                (VALUES
                {pairs_values}
            ) v (Overenskomst, TillægA, TillægB)
        ),
        
        AnsættelseTillæg AS (
            SELECT
                ans.AnsættelsesID,
                ans.Tjenestenummer,
                ans.Overenskomst,
                ans.Afdeling,
                org.LOSID,
                ans.Institutionskode,
                perstam.Navn,
                til.Tillægsnummer,
                til.Tillægsnavn,
                p.TillægA,
                p.TillægB
            FROM
                [Personale].[sd_magistrat].Ansættelse_mbu ans
            JOIN
                [Personale].[sd_magistrat].[tillæg_mbu] til
                    ON ans.AnsættelsesID = til.AnsættelsesID
            JOIN
                TillægPairs p
                    ON ans.Overenskomst = p.Overenskomst
                AND til.Tillægsnummer IN (p.TillægA, p.TillægB)
            JOIN
                [Personale].[sd].[personStam] perstam
                    ON ans.CPR = perstam.CPR
            LEFT JOIN
                [Personale].[sd].[Organisation] org
                    ON ans.Afdeling = org.SDafdID
            WHERE
                ans.Slutdato > GETDATE()
                AND ans.Startdato < GETDATE()
                AND ans.Statuskode IN ('1','3','5')
        		AND til.Slutdato > GETDATE()
        ),

        BrokenPairs AS (
            SELECT
                AnsættelsesID,
                TillægA,
                TillægB
            FROM
                AnsættelseTillæg
            GROUP BY
                AnsættelsesID,
                TillægA,
                TillægB
            HAVING
                COUNT(DISTINCT Tillægsnummer) = 1
        )

        SELECT
            a.Overenskomst,
            a.Tjenestenummer,
            a.Tillægsnummer,
            a.Tillægsnavn,
            a.Afdeling,
            a.LOSID,
            a.Navn,
            a.Institutionskode
        FROM
            AnsættelseTillæg a
        JOIN
            BrokenPairs b
                ON a.AnsættelsesID = b.AnsættelsesID
            AND a.TillægA = b.TillægA
            AND a.TillægB = b.TillægB
        ORDER BY
            a.Overenskomst, a.Tjenestenummer
    """

    logger.info(f"\n\nprinting full sql:\n\n{sql}\n\n")

    items = proces_helpers.get_items_from_query(connection_string_faelles_sql, sql)

    logger.info(f"len of items: {len(items)}")

    # Nothing found → stop early
    if not items:
        logger.info("KV2: No tillægsnummer issues found. Returning empty.")
        return []

    # -----------------------------
    # Enrichment: LOSID → Enhedsnavn (MBU/LIS DB)
    # -----------------------------
    items_df = pd.DataFrame(items)
    items_df["LOSID"] = items_df["LOSID"].astype(int, errors="ignore")

    lis_dep = proces_helpers.lis_enheder(connection_string=connection_string_mbu)
    lis_df = pd.DataFrame(lis_dep).rename(
        columns={"losid": "LOSID", "enhnavn": "Enhedsnavn"}
    )
    lis_df = lis_df[~lis_df["LOSID"].isna()].copy(deep=True)
    lis_df["LOSID"] = lis_df["LOSID"].astype(int, errors="ignore")

    items_dep = pd.merge(
        left=items_df,
        right=lis_df[["LOSID", "Enhedsnavn"]],
        how="left",
        on="LOSID",
    )[
        [
            "Tjenestenummer",
            "Tillægsnummer",
            "Tillægsnavn",
            "Overenskomst",
            "Afdeling",
            "Enhedsnavn",
            "Navn",
            "Institutionskode",
        ]
    ]

    return list(items_dep.T.to_dict().values())
