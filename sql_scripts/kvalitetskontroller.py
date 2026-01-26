"""Functions that defines errors to be handled by the robot"""

import sys
import os

import logging

from pathlib import Path

from datetime import date, timedelta

from collections import defaultdict

import pandas as pd

from helpers import helper_functions
from helpers.process_constants import PROCESS_CONSTANTS

logger = logging.getLogger(__name__)


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

    items = helper_functions.get_items_from_query(connection_string, sql)

    if items and af_receiver:
        item_df = pd.DataFrame(items).astype({"LOSID": int}, errors="ignore")

        items = helper_functions.combine_with_af_email(item_df=item_df)

    return items


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
    pairs_values = helper_functions.build_tillaeg_pairs_cte(tillaegsnr_par)

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

    items = helper_functions.get_items_from_query(connection_string_faelles_sql, sql)

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

    lis_dep = helper_functions.lis_enheder(connection_string=connection_string_mbu)
    lis_df = (
        pd.DataFrame(lis_dep)
        .rename(columns={"losid": "LOSID", "enhnavn": "Enhedsnavn"})
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


def kv3(
    accept_ovk_dag: tuple,
    accept_ovk_skole: tuple,
):
    """Ansættelser with wrong overenskomst based on departmentype"""

    connection_string_mbu = PROCESS_CONSTANTS["DBCONNECTIONSTRINGPROD"]
    connection_string_faelles = PROCESS_CONSTANTS["FaellesDbConnectionString"]

    # Load department types from LIS stamdata
    lis_stamdata = helper_functions.lis_enheder(
        connection_string=connection_string_mbu, afdtype=(2, 3, 4, 5, 11, 13)
    )
    losid_tuple = tuple(i["losid"] for i in lis_stamdata)

    # Load corresponding SD department codes
    sd_departments = helper_functions.sd_enheder(
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
            &
            (combined_df["SDafdID"].notna())
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

    items = helper_functions.get_items_from_query(connection_string=connection_str, query=sql)

    return items


def kv3_dev(
    accept_ovk_dag: tuple,
    accept_ovk_skole: tuple,
):
    """Ansættelser with wrong overenskomst based on departmentype"""

    connection_string_mbu = PROCESS_CONSTANTS["DBCONNECTIONSTRINGPROD"]
    connection_string_faelles = PROCESS_CONSTANTS["FaellesDbConnectionString"]

    # Load department types from LIS stamdata
    lis_stamdata = helper_functions.lis_enheder(
        connection_string=connection_string_mbu, afdtype=(2, 3, 4, 5, 11, 13)
    )
    losid_tuple = tuple(i["losid"] for i in lis_stamdata)

    # Load corresponding SD department codes
    sd_departments = helper_functions.sd_enheder(
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
            & ~(combined_df["SDafdID"].isna())
        )
    ]
    dagtilbud_afd = tuple(dagtilbud_df["SDafdID"].values)

    skole_df = combined_df[
        ((combined_df["afdtype"].isin([13])) & ~(combined_df["SDafdID"].isna()))
    ]
    skole_afd = tuple(skole_df["SDafdID"].values)

    # Collect ansættelser with wrong overenskomst
    items = kv3_dev_1(
        connection_str=connection_string_faelles,
        skole_afd=skole_afd,
        dagtilbud_afd=dagtilbud_afd,
        accept_ovk_skole=accept_ovk_skole,
        accept_ovk_dag=accept_ovk_dag,
    )
    items_df = pd.DataFrame(items)

    # # Get AF emails (probably just send to lønservice)
    # af_email = af_losid(connection_str=connection_string_mbu)
    # af_email_df = pd.DataFrame(af_email)
    # combined_df = pd.merge(left=combined_df, right=af_email_df, on="LOSID")

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


def kv3_dev_1(
    connection_str: str,
    skole_afd: tuple,
    dagtilbud_afd: tuple,
    accept_ovk_dag: tuple,
    accept_ovk_skole: tuple,
):
    """Get wrong overenskomst in skole and dagtilbud respectively"""
    accept_dag_str = (
        f"and Overenskomst not in {accept_ovk_dag}" if len(accept_ovk_dag) != 0 else ""
    )
    accept_skole_str = (
        f"and Overenskomst not in {accept_ovk_skole}"
        if len(accept_ovk_skole) != 0
        else ""
    )
    sql = f"""
        SELECT
            ans.Tjenestenummer, ans.Overenskomst, ans.Afdeling, ans.Institutionskode, perstam.Navn, ans.Startdato, ans.Slutdato, ans.Statuskode
        FROM
            [Personale].[sd_magistrat].[Ansættelse_mbu] ans
            left join [Personale].[sd].[personStam] as perstam
            on ans.CPR = perstam.CPR
        WHERE
            ((
                Afdeling in {dagtilbud_afd}
                and SUBSTRING(Overenskomst,1,1) = '7'
                and Overenskomst not in (76001, 76101)
                {accept_dag_str}
            )
            or
            (
                Afdeling in {skole_afd}
                and SUBSTRING(Overenskomst,1,1) = '4'
                and Overenskomst not in (46001, 46101)
                {accept_skole_str}
            ))
            and Statuskode in ('1', '3', '5')
            and Startdato <= GETDATE()
            and Slutdato > GETDATE()
    """
    items = helper_functions.get_items_from_query(connection_string=connection_str, query=sql)
    return items


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

    items = helper_functions.get_items_from_query(connection_string, sql)
    if items and af_receiver:
        item_df = pd.DataFrame(items).astype({"LOSID": int}, errors="ignore")

        items = helper_functions.combine_with_af_email(item_df=item_df)

    return items


def kv5():
    """
    Runs the KV5 payroll placement validation.

    Reads SISPO payroll files, extracts employee occurrences, retrieves
    active XA employments from SQL, loads TRIO→SD mapping rules, and
    validates whether each employee appears in the correct TRIO school.
    """

    # --------------------------------------------------
    # Configuration – Only process files from last full week
    # --------------------------------------------------
    today = date.today()

    # Monday of the current week (weekday(): Monday=0 ... Sunday=6)
    current_week_monday = today - timedelta(days=today.weekday())

    # Monday and Sunday of last week
    last_week_monday = current_week_monday - timedelta(days=7)
    last_week_sunday = last_week_monday + timedelta(days=6)

    print(f"Processing SISPO folders from: {last_week_monday} → {last_week_sunday}")

    fields = {
        "Institutionskode": (0, 2),
        "Tjenestenummer": (6, 11),
    }

    connection_string_mbu = PROCESS_CONSTANTS["DBCONNECTIONSTRINGPROD"]
    connection_string_faelles = PROCESS_CONSTANTS["FaellesDbConnectionString"]

    root_folder = Path(r"/data")

    # --------------------------------------------------
    # Phase 1: Read payroll files (NO SQL)
    # --------------------------------------------------
    records = []
    tjenestenumre = set()
    trio_school_codes = set()

    run_folders = []

    for p in root_folder.iterdir():
        try:
            if not p.name.startswith("MBU_Trio_"):
                continue

            # Extract date from folder name
            date_part = p.name.split("_")[2]
            folder_date = date(
                int(date_part[0:4]),
                int(date_part[4:6]),
                int(date_part[6:8]),
            )

            # Only process folders from LAST WEEK (Mon–Sun)
            if not (last_week_monday <= folder_date <= last_week_sunday):
                continue

            run_folders.append((folder_date, p))

        except Exception:
            continue

    run_folders.sort(key=lambda x: x[0], reverse=True)

    for folder_date, run_folder in run_folders:

        sispo_files = []

        for p in run_folder.iterdir():
            try:
                if p.name.upper().startswith("SISPO"):
                    sispo_files.append(p)
            except OSError:
                continue

        sispo_files.sort(key=lambda p: p.name, reverse=True)

        for file_path in sispo_files:

            print(f"Reading file: {run_folder.name} / {file_path.name}")
            print()

            parts = file_path.name.split("-")

            if len(parts) < 4:
                raise RuntimeError(
                    f"Unexpected SISPO filename format: {file_path.name}"
                )

            trio_school_code = parts[2]
            trio_school_codes.add(trio_school_code)

            with file_path.open("r", encoding="utf-8", errors="replace") as file:

                for line_no, raw_line in enumerate(file, start=1):

                    line = raw_line.rstrip("\n")

                    if not line.strip():
                        continue

                    record = {
                        "Folder_date": folder_date,
                        "Trio_school_code": trio_school_code,
                        "File_name": file_path.name,
                        "Line_no": line_no,
                    }

                    for key, (start, end) in fields.items():
                        record[key] = line[start:end].strip()

                    tjenestenumre.add(record["Tjenestenummer"])

                    validate_record(
                        record=record,
                        file_name=file_path.name,
                        line_no=line_no
                    )

                    records.append(record)

    print(f"Parsed records        : {len(records)}")
    print(f"Distinct employees    : {len(tjenestenumre)}")
    print(f"Distinct TRIO schools : {len(trio_school_codes)}")
    print()

    # --------------------------------------------------
    # Phase 2: Fetch ACTIVE XA employments (ONE SQL)
    # --------------------------------------------------
    emp_placeholders = ",".join("?" for _ in tjenestenumre)

    employee_sql = f"""
        WITH ActiveEmployments AS (
            SELECT
                per.[Navn],
                ans.[Tjenestenummer],
                ans.[AnsættelsesID],
                ans.[Institutionskode],
                ans.Overenskomst,
                ans.[Afdeling],

                COUNT(*) OVER (
                    PARTITION BY ans.[Tjenestenummer]
                ) AS active_count

            FROM
                [Personale].[sd_magistrat].[Ansættelse_mbu] ans
            LEFT JOIN
                [Personale].[sd].[personStam] per
                    ON ans.CPR = per.CPR
            WHERE
                ans.[Tjenestenummer] IN ({emp_placeholders})
                AND ans.[Institutionskode] = 'XA'
                AND ans.Startdato <= CAST(GETDATE() AS date)
                AND (
                    ans.Slutdato IS NULL
                    OR ans.Slutdato > CAST(GETDATE() AS date)
                )
        )

        SELECT
            Navn,
            Tjenestenummer,
            AnsættelsesID,
            Institutionskode,
            Overenskomst,
            Afdeling,
            active_count
        FROM ActiveEmployments
    """

    employee_rows = helper_functions.get_items_from_query_with_params(
        connection_string=connection_string_faelles,
        query=employee_sql,
        params=list(tjenestenumre)
    )

    employees_by_tjenestenummer = {}
    multiple_active_employments = []

    for row in employee_rows:
        tjenestenummer = row["Tjenestenummer"]

        if row["active_count"] > 1:
            multiple_active_employments.append(row)
            continue

        employees_by_tjenestenummer[tjenestenummer] = row

    # --------------------------------------------------
    # Phase 2.5: Master data consistency errors
    # --------------------------------------------------
    mismatches = []
    seen = set()

    for row in multiple_active_employments:
        tjenestenummer = row["Tjenestenummer"]
        key = (tjenestenummer, "MULTIPLE_ACTIVE_EMPLOYMENTS")

        if key in seen:
            continue

        mismatches.append({
            "Tjenestenummer": tjenestenummer,
            "Overenskomst": row["Overenskomst"],
            "Navn": row["Navn"],
            "Institutionskode": row["Institutionskode"],
            "Afdeling": row["Afdeling"],
            "Error": "MULTIPLE_ACTIVE_EMPLOYMENTS"
        })
        seen.add(key)

    # --------------------------------------------------
    # Phase 3: Fetch TRIO → SD mappings (ONE SQL)
    # --------------------------------------------------
    trio_placeholders = ",".join("?" for _ in trio_school_codes)

    mapping_sql = f"""
        SELECT
            [SKOLEKODE],
            [SDafdID]
        FROM
            [RPA].[rpa].[TRIO_Skolekoder]
        WHERE
            [SKOLEKODE] IN ({trio_placeholders})
    """

    mapping_rows = helper_functions.get_items_from_query_with_params(
        connection_string=connection_string_mbu,
        query=mapping_sql,
        params=list(trio_school_codes)
    )

    trio_to_sd = defaultdict(set)

    for row in mapping_rows:
        trio_to_sd[str(row["SKOLEKODE"])].add(row["SDafdID"])

    # --------------------------------------------------
    # Phase 4: Payroll placement validation
    # --------------------------------------------------
    for record in records:
        tjenestenummer = record["Tjenestenummer"]
        trio_school_code = record["Trio_school_code"]

        employee = employees_by_tjenestenummer.get(tjenestenummer)

        if not employee:
            key = (tjenestenummer, "NO_ACTIVE_XA_EMPLOYMENT")

            if key not in seen:
                mismatches.append({
                    **record,
                    "Error": "NO_ACTIVE_XA_EMPLOYMENT"
                })
                seen.add(key)

            continue

        afdeling = employee["Afdeling"]
        allowed_sd = trio_to_sd.get(trio_school_code, set())

        if afdeling not in allowed_sd:
            key = (tjenestenummer, trio_school_code, "SD_NOT_VALID_FOR_TRIO")

            if key not in seen:
                mismatches.append({
                    **record,
                    "Afdeling": afdeling,
                    "Allowed_sd": sorted(allowed_sd),
                    "Navn": employee.get("Navn"),
                    "Overenskomst": employee.get("Overenskomst"),
                    "Error": "SD_NOT_VALID_FOR_TRIO"
                })
                seen.add(key)

    # --------------------------------------------------
    # Output
    # --------------------------------------------------
    print(f"Mismatches found: {len(mismatches)}")
    print()

    for row in mismatches:
        print(row)
        print()

    return mismatches


def validate_record(record: dict, file_name: str, line_no: int):
    """
    Raises ValueError if record does not match expected pattern.
    """

    if record["Institutionskode"] != "XA":
        raise ValueError(
            f"{file_name} | line {line_no}: key1 must be 'XA', got '{record['Institutionskode']}'"
        )

    if not (record["Tjenestenummer"].isdigit() and len(record["Tjenestenummer"]) == 5):
        raise ValueError(
            f"{file_name} | line {line_no}: key2 must be 7-digit number, got '{record['Tjenestenummer']}'"
        )
