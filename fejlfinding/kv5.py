"""KV5: finder lønrækker i SISPO-lønudtræk tilkoblet forkert TRIO-skolekode"""

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from helpers import proces_helpers
from helpers.process_constants import PROCESS_CONSTANTS
from processes.error_handling import send_error_email

logger = logging.getLogger(__name__)


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
        "Lønår": (15, 17),
        "Lønmåned": (17, 19),
        "Løndato": (21, 23),
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

                    record["Wage date"] = date.fromisoformat(
                        f"20{record['Lønår']}{record['Lønmåned']}{record['Løndato']}"
                    )

                    tjenestenumre.add(record["Tjenestenummer"])
                    dates.add(
                        record["File date"]
                    )  # Tilføj både løndato og indebretningsdato til tjek
                    dates.add(record["Wage date"])

                    proces_helpers.validate_record(
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
                per.[CPR],
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
            CPR,
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

    employee_rows = proces_helpers.get_items_from_query_with_params(
        connection_string=connection_string_faelles,
        query=employee_sql,
        params=list(tjenestenumre),
    )

    employee_df = pd.DataFrame(employee_rows)

    # Opdel ansættelser i aktive/ikke-aktive
    active_list = ["1", "3", "5"]
    active_employees = employee_df[employee_df["Statuskode"].isin(active_list)]

    # Alle ansættelsesperioder på et CPR-nr.
    cpr_employment_sql = """
        WITH Ansaettelseslinjer AS (
            SELECT
                per.[Navn],
                ans.[Tjenestenummer],
                ans.[Institutionskode],
                ans.[Afdeling],
                ans.[Startdato],
                ans.[Slutdato],
                ans.[Statuskode],
                sta.[StatusTekst]
            FROM
                [Personale].[sd_magistrat].[Ansættelse_mbu] ans
            LEFT JOIN
                [Personale].[sd].[Statuskode] sta
                    ON ans.Statuskode = sta.Statuskode
            LEFT JOIN
                [Personale].[sd].[personStam] per
                    ON ans.CPR = per.CPR
            WHERE
                ans.[CPR] = ?
        )
        SELECT
            Tjenestenummer,
            Institutionskode,
            Afdeling,
            Startdato,
            Slutdato,
            StatusTekst
        FROM
            Ansaettelseslinjer
        WHERE
            CAST(Slutdato AS date) > DATEADD(year, -1, CAST(GETDATE() AS date))
        ORDER BY
            Startdato DESC
    """

    logger.info(
        f"XA employments fetched for relevant period ({min_date_placeholder} - {max_date_placeholder})"
    )

    # # --------------------------------------------------
    # Phase 3: Fetch TRIO → SD mappings (ONE SQL)
    # --------------------------------------------------
    mismatches_by_key = {}
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

    mapping_rows = proces_helpers.get_items_from_query_with_params(
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
        wage_date = record[
            "Wage date"
        ]  # Ligner måske at den ikke bruges, men bruges i queries

        in_data = employee_df.query("Tjenestenummer == @tjenestenummer")

        # Find aktive ansættelser på tjenestenummeret på indberetnings- og løndatoen
        active_on_record = active_employees.query(
            "Tjenestenummer == @tjenestenummer and Startdato <= @record_date <= Slutdato"
        )
        active_on_wage = active_employees.query(
            "Tjenestenummer == @tjenestenummer and Startdato <= @wage_date <= Slutdato"
        )

        emp_navn = "Ukendt"
        # Ved inaktiv på én af datoerne
        if len(active_on_record) == 0 or len(active_on_wage) == 0:
            # Hvis slet ikke i løndata
            if len(in_data) == 0:
                error_type = "NO_EMPLOYMENT"
            # Hvis inaktiv på løndatoen
            elif len(active_on_wage) == 0:
                error_type = "EMPLOYMENT_NOT_ACTIVE_ON_WAGE"
                latest_employment = in_data.sort_values("Startdato").iloc[-1]
                emp_navn = latest_employment["Navn"]
            # Hvis ansættelsen ikke er aktiv på indberetningsdatoen, men på løndatoen
            elif len(active_on_record) == 0 and len(active_on_wage) > 0:
                # len(active_on_wage) vil som regel være én, hvis der er aktiv ansættelse på løndatoen,
                # men få ansættelser har to rækker, fordi de har skiftet CPR nummer i løbet af deres ansættelse
                error_type = "EMPLOYMENT_NOT_ACTIVE_ON_RECORD_ACTIVE_ON_WAGE"
                latest_employment = in_data.sort_values("Startdato").iloc[-1]
                emp_navn = latest_employment["Navn"]

            # Én mismatch per tjenestenummer og fejltype. Fælles oplysninger
            # (navn og ansættelser på CPR-nr.) slås kun op første gang.
            key = (tjenestenummer, error_type)
            if key not in mismatches_by_key:
                mismatch = {
                    "Records": [],
                    "Tjenestenummer": tjenestenummer,
                    "Navn": emp_navn,
                    "Error": error_type,
                    "record_status": [],
                    "wage_status": [],
                    "cpr_employments": [],
                }

                if error_type == "EMPLOYMENT_NOT_ACTIVE_ON_WAGE":
                    # Ansættelser på det CPR-nr. der senest var tilknyttet tjenestenummeret
                    mismatch["cpr_employments"] = (
                        proces_helpers.get_items_from_query_with_params(
                            connection_string=connection_string_faelles,
                            query=cpr_employment_sql,
                            params=[latest_employment["CPR"]],
                        )
                    )

                mismatches_by_key[key] = mismatch

            mismatch = mismatches_by_key[key]

            # Hver fejlende lønrække bliver en række i mailens tabel
            mismatch["Records"].append({**record})

            # # Tjenestenummerets ansættelser på hver forskellig indberetnings- og løndato
            for status_key, status_date in (
                ("record_status", record_date),
                ("wage_status", wage_date),
            ):
                if all(s["Dato"] != status_date for s in mismatch[status_key]):
                    mismatch[status_key].append(
                        {
                            "Dato": status_date,
                            "Ansættelser": employee_df.query(
                                "Tjenestenummer == @tjenestenummer and Startdato <= @status_date <= Slutdato"
                            ).to_dict(orient="records"),
                        }
                    )

            continue

        employee = active_on_record.to_dict(orient="records")[0]
        afdeling = employee["Afdeling"]
        allowed_sd = trio_to_sd.get(trio_school_code, set())

        if afdeling not in allowed_sd:
            key = (tjenestenummer, trio_school_code, "SD_NOT_VALID_FOR_TRIO")

            if key not in mismatches_by_key:
                mismatches_by_key[key] = {
                    **record,
                    "Afdeling": afdeling,
                    "Allowed_sd": sorted(allowed_sd),
                    "Navn": employee.get("Navn"),
                    "Overenskomst": employee.get("Overenskomst"),
                    "Error": "SD_NOT_VALID_FOR_TRIO",
                }

    mismatches = list(mismatches_by_key.values())

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
