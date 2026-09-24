"""Functions that defines errors to be handled by the robot"""

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from helpers import helper_functions, kv6_support_functions, kv7_support_functions
from helpers.process_constants import PROCESS_CONSTANTS
from processes.error_handling import send_error_email

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

    items = helper_functions.get_items_from_query(
        connection_string=connection_str, query=sql
    )

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
    items = helper_functions.get_items_from_query(
        connection_string=connection_str, query=sql
    )
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
    logger.info("Reading payroll files")
    records = []
    tjenestenumre = set()
    trio_school_codes = set()
    dates = set()

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

    logger.info(f"Looking through {len(run_folders)} folders.")

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
            file_date = datetime.strptime(parts[1][:6], "%y%m%d").date()

            if not trio_school_code:
                try:
                    error_dict = {
                        "type": "Rejst manuelt",
                        "message": f"Trio filen indeholder ikke en Trio skole kode. \nFilnavn: {file_path}, TRIO kode fundet som tredje element i: {parts}",
                        "traceback": None,
                    }
                    send_error_email(
                        error=None,
                        process_name="SD Løn KV5 (TRIO-tjek)",
                        custom_error_dict=error_dict,
                    )
                    continue
                except Exception:
                    raise RuntimeError(
                        f"TRIO school code not found in {file_path}. Resolved to third part in {parts} as {trio_school_code}"
                    )

            trio_school_codes.add(trio_school_code)

            with file_path.open("r", encoding="utf-8", errors="replace") as file:
                for line_no, raw_line in enumerate(file, start=1):
                    line = raw_line.rstrip("\n")

                    if not line.strip():
                        continue

                    record = {
                        "Folder_date": folder_date,
                        "File date": file_date,
                        "Trio_school_code": trio_school_code,
                        "File_name": file_path.name,
                        "Line_no": line_no,
                    }

                    for key, (start, end) in fields.items():
                        record[key] = line[start:end].strip()

                    tjenestenumre.add(record["Tjenestenummer"])
                    dates.add(record["File date"])

                    validate_record(
                        record=record, file_name=file_path.name, line_no=line_no
                    )

                    records.append(record)

    logger.info("Payroll files read")
    print(f"Parsed records        : {len(records)}")
    print(f"Distinct employees    : {len(tjenestenumre)}")
    print(f"Distinct TRIO schools : {len(trio_school_codes)}")
    print()

    # --------------------------------------------------
    # Phase 2: Fetch ACTIVE XA employments (ONE SQL)
    # --------------------------------------------------
    logger.info("Fetching active XA employments")
    emp_placeholders = ",".join("?" for _ in tjenestenumre)

    # Første og sidste dato for lønindberetninger (baseret på filnavne)
    min_date_placeholder = min(dates)
    max_date_placeholder = max(dates)

    employee_sql = f"""
        WITH ActiveEmployments AS (
            SELECT
                per.[Navn],
                ans.[Tjenestenummer],
                ans.[AnsættelsesID],
                ans.[Institutionskode],
                ans.Overenskomst,
                ans.[Afdeling],
                ans.[Startdato],
                ans.[Slutdato],
                ans.[Statuskode],
                sta.[StatusTekst]

            FROM
                [Personale].[sd_magistrat].[Ansættelse_mbu] ans
            LEFT JOIN
                [Personale].[sd].[personStam] per
                    ON ans.CPR = per.CPR
            LEFT JOIN
                [Personale].[sd].[Statuskode] sta
                    ON ans.Statuskode = sta.Statuskode
            WHERE
                ans.[Tjenestenummer] IN ({emp_placeholders})
                AND ans.[Institutionskode] = 'XA'
        )
        SELECT
            Navn,
            Tjenestenummer,
            AnsættelsesID,
            Institutionskode,
            Overenskomst,
            Afdeling,
            Startdato,
            Slutdato,
            Statuskode,
            StatusTekst
        FROM ActiveEmployments
    """

    employee_rows = helper_functions.get_items_from_query_with_params(
        connection_string=connection_string_faelles,
        query=employee_sql,
        params=list(tjenestenumre),
    )

    employee_df = pd.DataFrame(employee_rows)

    # Opdel ansættelser i aktive/ikke-aktive
    active_list = ["1", "3", "5"]
    active_employees = employee_df[employee_df["Statuskode"].isin(active_list)]
    non_active_employees = employee_df[~employee_df["Statuskode"].isin(active_list)]

    logger.info(
        f"XA employments fetched for relevant period ({min_date_placeholder} - {max_date_placeholder})"
    )

    # # --------------------------------------------------
    # Phase 3: Fetch TRIO → SD mappings (ONE SQL)
    # --------------------------------------------------
    mismatches = []
    seen = set()
    logger.info("Fetching TRIO -> SD mappings")
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
        params=list(trio_school_codes),
    )

    trio_to_sd = defaultdict(set)

    for row in mapping_rows:
        trio_to_sd[str(row["SKOLEKODE"])].add(row["SDafdID"])

    logger.info("TRIO -> SD mappings fetched")
    # --------------------------------------------------
    # Phase 4: Payroll placement validation
    # --------------------------------------------------
    logger.info("Payroll placement validation")
    for record in records:
        tjenestenummer = record["Tjenestenummer"]
        trio_school_code = record["Trio_school_code"]
        record_date = record[
            "File date"
        ]  # Ligner måske at den ikke bruges, men bruges i queries

        employee = active_employees.query(
            "Tjenestenummer == @tjenestenummer and Startdato <= @record_date < Slutdato"
        )
        if len(employee) > 1:
            row = employee.to_dict(orient="records")[0]
            key = (tjenestenummer, "MULTIPLE_ACTIVE_EMPLOYMENTS")
            if key not in seen:
                mismatches.append(
                    {
                        "Tjenestenummer": tjenestenummer,
                        "Overenskomst": row["Overenskomst"],
                        "Navn": row["Navn"],
                        "Institutionskode": row["Institutionskode"],
                        "Afdeling": row["Afdeling"],
                        "Error": "MULTIPLE_ACTIVE_EMPLOYMENTS",
                    }
                )
                seen.add(key)
            continue

        if len(employee) == 0:  # Ikke aktiv ansættelse i den relevante periode
            non_active_row = non_active_employees.query(
                "Tjenestenummer == @tjenestenummer and Startdato <= @record_date < Slutdato"
            ).to_dict(orient="records")[0]
            if len(
                non_active_row
            ):  # Har en ikke-atkiv ansættelsesrække i relevant periode
                key = (tjenestenummer, "XA_EMPLOYMENT_NON_ACTIVE")
                # Find ansættelser på tjenestenumret uden for perioden
                other_employments = (
                    employee_df.query(
                        (
                            "Tjenestenummer == @tjenestenummer "
                            + "and (Startdato > @record_date or Slutdato <= @record_date)"
                        )
                    )
                    .sort_values("Slutdato", ascending=False)
                    .to_dict(orient="records")
                )
                if key not in seen:
                    mismatches.append(
                        {
                            **record,
                            "Navn": non_active_row["Navn"],
                            "status_text": non_active_row["StatusTekst"],
                            "non_active_start": non_active_row["Startdato"],
                            "non_active_end": non_active_row["Slutdato"],
                            "other_employments": other_employments,
                            "Error": "XA_EMPLOYMENT_NON_ACTIVE",
                        }
                    )
                    seen.add(key)

                continue

            key = (tjenestenummer, "NO_ACTIVE_XA_EMPLOYMENT")

            if key not in seen:
                mismatches.append({**record, "Error": "NO_ACTIVE_XA_EMPLOYMENT"})
                seen.add(key)

            continue

        employee = employee.to_dict(orient="records")[0]
        afdeling = employee["Afdeling"]
        allowed_sd = trio_to_sd.get(trio_school_code, set())

        if afdeling not in allowed_sd:
            key = (tjenestenummer, trio_school_code, "SD_NOT_VALID_FOR_TRIO")

            if key not in seen:
                mismatches.append(
                    {
                        **record,
                        "Afdeling": afdeling,
                        "Allowed_sd": sorted(allowed_sd),
                        "Navn": employee.get("Navn"),
                        "Overenskomst": employee.get("Overenskomst"),
                        "Error": "SD_NOT_VALID_FOR_TRIO",
                    }
                )
                seen.add(key)

    logger.info("Payroll placement validation completed")
    logger.info("KV5 logic completed")
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

    # Per 1/8-2026 findes der tjenestenumre der slutter på A
    # Fjerner den validering der kræver kun tal
    if not (
        # record["Tjenestenummer"].isdigit() and
        len(record["Tjenestenummer"]) == 5
    ):
        raise ValueError(
            f"{file_name} | line {line_no}: key2 must be 5-digits number, got '{record['Tjenestenummer']}'"
        )


def compare_wages_month_prior(tjenestenumre: tuple):
    """
    Sums up tillægs beløb and trin as well as grundtrin from current month and prior month
    """

    sql_query = f"""    
        WITH unified_now AS (
            -- Tillæg rows active now
            SELECT
                til.Tjenestenummer,
                til.AnsættelsesID,
                CASE WHEN til.Beløb <> 0 THEN til.Beløb END AS Beløb,
                CASE WHEN til.Beløb = 0  THEN til.Trin  END AS Trin,
                CASE WHEN til.Enhed = 0 THEN 1 ELSE til.Enhed END AS Enhed
            FROM [Personale].[sd_magistrat].[tillæg_mbu] AS til
            WHERE til.Startdato <= DATEADD(MONTH, 0, GETDATE())
            AND til.Slutdato  >  DATEADD(MONTH, 0, GETDATE())
            AND til.Institutionskode = 'XA'

            UNION ALL

            -- Ansættelse rows active now
            SELECT
                ans.Tjenestenummer,
                ans.AnsættelsesID,
                NULL AS Beløb,
                ans.Trin AS Trin,
                NULL as Enhed
            FROM [Personale].[sd_magistrat].[Ansættelse_mbu] AS ans
            WHERE ans.Startdato <= DATEADD(MONTH, 0, GETDATE())
            AND ans.Slutdato  >  DATEADD(MONTH, 0, GETDATE())
            AND ans.Statuskode IN ('1','3','5')
            AND ans.Institutionskode = 'XA'
        ),
        agg_now AS (
            SELECT
                Tjenestenummer,
                AnsættelsesID,
                COALESCE(SUM(Beløb*Enhed), 0) AS Sum_Beløb,
                COALESCE(SUM(Trin), 0)  AS Sum_Trin
            FROM unified_now
            GROUP BY Tjenestenummer, AnsættelsesID
        ),

        unified_prev AS (
            -- Tillæg rows active one month back
            SELECT
                til.Tjenestenummer,
                til.AnsættelsesID,
                CASE WHEN til.Beløb <> 0 THEN til.Beløb END AS Beløb,
                CASE WHEN til.Beløb = 0  THEN til.Trin  END AS Trin,
                CASE WHEN til.Enhed = 0 THEN 1 ELSE til.Enhed END AS Enhed
            FROM [Personale].[sd_magistrat].[tillæg_mbu] AS til
            WHERE til.Startdato <= DATEADD(MONTH, -1, GETDATE())
            AND til.Slutdato  >  DATEADD(MONTH, -1, GETDATE())
            AND til.Institutionskode = 'XA'

            UNION ALL

            -- Ansættelse rows active one month back
            SELECT
                ans.Tjenestenummer,
                ans.AnsættelsesID,
                NULL AS Beløb,
                ans.Trin AS Trin,
                NULL as Enhed
            FROM [Personale].[sd_magistrat].[Ansættelse_mbu] AS ans
            WHERE ans.Startdato <= DATEADD(MONTH, -1, GETDATE())
            AND ans.Slutdato  >  DATEADD(MONTH, -1, GETDATE())
            AND ans.Statuskode IN ('1','3','5')
            AND ans.Institutionskode = 'XA'
        ),
        agg_prev AS (
            SELECT
                Tjenestenummer,
                AnsættelsesID,
                COALESCE(SUM(Beløb*Enhed), 0) AS Sum_Beløb_Prev,
                COALESCE(SUM(Trin), 0)  AS Sum_Trin_Prev
            FROM unified_prev
            GROUP BY Tjenestenummer, AnsættelsesID
        )

        SELECT
            COALESCE(n.Tjenestenummer, p.Tjenestenummer)  AS Tjenestenummer,
            COALESCE(n.AnsættelsesID, p.AnsættelsesID)    AS AnsættelsesID,

            n.Sum_Beløb,
            p.Sum_Beløb_Prev,
            n.Sum_Trin,
            p.Sum_Trin_Prev,
            CAST(ISNULL(n.Sum_Beløb, 0) - ISNULL(p.Sum_Beløb_Prev, 0) AS decimal(18, 2)) AS Delta_Beløb,
            ISNULL(n.Sum_Trin, 0) - ISNULL(p.Sum_Trin_Prev, 0) AS Delta_Trin
        FROM agg_now AS n
        FULL OUTER JOIN agg_prev AS p
        ON  p.Tjenestenummer = n.Tjenestenummer
        AND p.AnsættelsesID  = n.AnsættelsesID
        WHERE p.Tjenestenummer in {tjenestenumre}
    """

    connection_string = PROCESS_CONSTANTS["FaellesDbConnectionString"]

    rows = helper_functions.get_items_from_query(
        connection_string=connection_string, query=sql_query
    )

    return rows


def kv6():
    """
    Get monthly wages for current and previous month for a group of employees, and check that wages haven't changed.
    """

    conn_str_mbu = PROCESS_CONSTANTS["DBCONNECTIONSTRINGPROD"]

    tjenestenumre_df = kv6_support_functions.get_tjenestenumre(conn_str_mbu)

    tjenestenumre = tuple(tjenestenumre_df["Tjenestenummer"].values)

    wage_data = compare_wages_month_prior(tjenestenumre=tjenestenumre)

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

    items = helper_functions.item_df_to_item_list(item_df=items_df)

    return items


def kv7(exclude_schoolname: list, exclude_dagtilbudname: list):
    """
    Function to check that employees have necesarry tillæg
    """

    conn_str_faellesdb = PROCESS_CONSTANTS["FaellesDbConnectionString"]
    conn_str_mbu = PROCESS_CONSTANTS["DBCONNECTIONSTRINGPROD"]
    # Get employees
    employees_df = kv7_support_functions.get_employees(conn_str_faellesdb)

    # Select almen
    almen_employees_df = kv7_support_functions.select_employees_almen(
        conn_str_mbu,
        conn_str_faellesdb,
        exclude_schoolname,
        exclude_dagtilbudname,
        employees_df,
    )

    # Select only school employees for now
    almen_employees_df = almen_employees_df[almen_employees_df["Enhedstype"] == "Skole"]

    # Get mapping of tillæg and lønklasser
    minimumstillaeg_df = kv7_support_functions.get_minimumstillaeg(conn_str_mbu)

    # Compare
    employee_missing_tillaeg = kv7_support_functions.check_employee_tillaeg(
        employee_df=almen_employees_df, minimumstillaeg_df=minimumstillaeg_df
    )

    items = helper_functions.item_df_to_item_list(employee_missing_tillaeg)

    return items
