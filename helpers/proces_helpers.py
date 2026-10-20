"""Hjælpefunktioner til kvalitetskontrollerne og deres notifikationer"""

import json
import logging
from datetime import datetime

import pandas as pd
import pyodbc

from helpers import ats_helpers
from helpers.process_constants import PROCESS_CONSTANTS

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Databaseopslag
# --------------------------------------------------


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
    connection_string, query: str, params: list | tuple
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


# --------------------------------------------------
# Enheder og AF
# --------------------------------------------------


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
        sql += f"""
                WHERE
                    afdtype IN {afdtype}
            """

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
        sql += f"""
                WHERE
                    LOSID IN {losid_tuple}
            """

    departments = get_items_from_query(connection_string=connection_string, query=sql)

    return departments


def af_losid(connection_str: str):
    """Get AF per LOSID"""

    sql = """
    SELECT
        losidEnhed as LOSID,
        emailAF as AF_email
    FROM [RPA].[rpa].[MBU_AF]
    """

    af_email_kobling = get_items_from_query(connection_string=connection_str, query=sql)

    return af_email_kobling


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

    items = ats_helpers.item_df_to_item_list(item_df=combined_df)

    return items


# --------------------------------------------------
# Tillæg
# --------------------------------------------------


def build_tillaeg_pairs_cte(pairs: list[dict]) -> str:
    rows = []
    for p in pairs:
        ovk = p["ovk"]
        a, b = p["pair"]
        rows.append(f"({ovk}, {a}, {b})")
    return ",\n".join(rows)


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

    pair = data.get("pair")
    pair_names = data.get("pair_names")

    if (
        isinstance(pair, tuple)
        and isinstance(pair_names, tuple)
        and len(pair) == 2
        and len(pair_names) == 2
    ):
        if number in pair:
            index = pair.index(number)
            other_index = 1 - index

            return pair[other_index], pair_names[other_index]

    return None


def get_tillaeg_navn(conn_str_faelles: str, tillaeg: int | str):
    sql = f"""
        SELECT
	        Tillægsnummer, Tillægsnavn
        FROM Personale.sd_magistrat.tillæg_mbu
        WHERE 
            Tillægsnummer = {tillaeg} 
            and Startdato <= getdate() and Slutdato > getdate()
        GROUP BY Tillægsnummer, Tillægsnavn
    """
    tillaeg_navn = get_items_from_query(conn_str_faelles, sql)

    assert len(tillaeg_navn) == 1, (
        f"The tillaegsnummer returned {len(tillaeg_navn)} different names associated with the nummer"
    )

    return tillaeg_navn[0]


# --------------------------------------------------
# Lønrækker og lønsammenligning
# --------------------------------------------------


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


def get_tjenestenumre(conn_str: str) -> pd.DataFrame:
    sql = """
        SELECT
            *
        FROM
            [RPA].[sdlon].[gl_fleksordning]
    """
    items = get_items_from_query(conn_str, sql)
    items_df = pd.DataFrame(items)

    return items_df


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

    rows = get_items_from_query(
        connection_string=connection_string, query=sql_query
    )

    return rows


# --------------------------------------------------
# Minimumstillæg
# --------------------------------------------------


def check_employee_tillaeg(
    employee_df: pd.DataFrame,
    # dev_employee_df: pd.DataFrame,
    minimumstillaeg_df: pd.DataFrame,
):
    """
    Check that every employee has the required tillæg
    """

    # Drops all employees with loenklasse with no specified minimum requirements
    combined_df = pd.merge(
        left=employee_df,
        right=minimumstillaeg_df,
        left_on=["Lønklasse", "Institutionskode", "Enhedstype", "Stilling"],
        right_on=["Loenklasse", "SD_Institutionskode", "Enhedstype", "Stilling"],
        how="left",
    )

    missing_tillaeg = find_missing_tillaeg(
        combined_df, cur_col="Tillaeg_List", req_col="required_tillaeg_list"
    )

    return missing_tillaeg


def find_missing_tillaeg(df: pd.DataFrame, cur_col: str, req_col: str) -> pd.DataFrame:
    """Check which rows are missing required tillæg.

    Returns a dataframe containing only rows that miss
    one or more required tillæg, with a column listing the missing ones.
    """

    def extract_current(json_str: str):
        """Parse JSON and return a set of Tillægsnummer values."""
        if not isinstance(json_str, str) or not json_str.strip():
            return set()
        items = json.loads(json_str)
        return {int(item["Tillægsnummer"]) for item in items}

    def extract_required(json_str: str):
        if not isinstance(json_str, str) or not json_str.strip():
            return set()
        items = json.loads(json_str)
        return {int(item) for item in items}

    # Extract available tillaeg from JSON
    df = df.copy()
    df["available_tillaeg"] = df[cur_col].apply(extract_current)
    df["required_tillaeg"] = df[req_col].apply(extract_required)

    # Compute missing tillaeg
    df["missing_tillaeg"] = df.apply(
        lambda row: list(set(row["required_tillaeg"]) - set(row["available_tillaeg"])),
        axis=1,
    )

    # Filter: keep rows with missing values
    return df[df["missing_tillaeg"].map(len) > 0]


def get_minimumstillaeg(conn_str: str):
    """
    Get minimumstillæg from database (original source is ELA-tragt)
    """
    sql = """
        SELECT 
            [SD_Institutionskode]
            ,[Ansaettelsesform]
            ,[Enhedstype]
            ,[Stilling]
            ,[SD_Stilling]
            ,[Loenklasse]
            ,[Tillaegs_nummer_SD] as required_tillaeg_list
        FROM 
            [RPA].[sdlon].[minimumstillaeg]
    """

    items = get_items_from_query(conn_str, sql)
    items_df = pd.DataFrame(items)

    items_df["Stilling"] = items_df["Stilling"].replace(
        "Pædagmedhjælper", "Pædagogmedhjælper"
    )

    return items_df


def get_schools(connection_string_mbu: str, connection_string_faelles: str):
    # Load department types from LIS stamdata
    lis_stamdata = lis_enheder(
        connection_string=connection_string_mbu, afdtype=(2, 3, 4, 5, 11, 13)
    )
    losid_tuple = tuple(i["losid"] for i in lis_stamdata)

    # Load corresponding SD department codes
    sd_departments = sd_enheder(
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

    combined_df["Enhedstype"] = None

    combined_df.loc[combined_df["afdtype"] == 13, "Enhedstype"] = "Skole"
    combined_df.loc[combined_df["afdtype"].isin([2, 3, 4, 5, 11]), "Enhedstype"] = (
        "Dagtilbud"
    )

    return combined_df


def select_employees_almen(
    conn_str_mbu: str,
    conn_str_faelles: str,
    exclude_schoolname: list,
    exclude_dagtilbudname: list,
    employees_df: pd.DataFrame,
):
    # Filter employees working in schools
    inst_df = get_schools(conn_str_mbu, conn_str_faelles)
    inst_employee_df = pd.merge(
        left=inst_df,
        right=employees_df,
        left_on="SDafdID",
        right_on="Afdeling",
        how="inner",
    )
    inst_employee_df = inst_employee_df[
        [
            "AnsættelsesID",
            "Institutionskode",
            "Tjenestenummer",
            "Stilling",
            "Stillingskode",
            "Lønklasse",
            "Tillaeg_List",
            "enhnavn",
            "SDafdID",
            "Enhedstype",
        ]
    ]

    # Get schoolnames
    sql = """
        SELECT
            *
        FROM 
            [RPA].[rpa].[TRIO_Skolekoder]
    """

    items = get_items_from_query(conn_str_mbu, sql)
    items_df = pd.DataFrame(items)

    # Filter schools (exclude special schools)
    almen_employees_df = pd.merge(
        left=inst_employee_df,
        right=items_df,
        on="SDafdID",
        how="left",
    )
    almen_employees_df = almen_employees_df[
        (~almen_employees_df["SKOLENAVN"].isin(exclude_schoolname))
    ]

    return almen_employees_df


def get_employees(conn_str: str):
    sql = """
        with ans as (
            SELECT 
                [AnsættelsesID]
                ,[Institutionskode]
                ,[Tjenestenummer]
                ,[Ansættelsesdato]
                ,[Startdato]
                ,[Slutdato]
                ,[Afdeling]
                ,[Stillingskode]
                ,[Stilling]
                ,[Tjenestekode]
                ,[Overenskomst]
                ,[Lønklasse]
                ,[Trin]
                ,[Anciennitetsdato]
                ,[Statuskode]
                ,[Ansættelsestype]
            FROM 
                [Personale].[sd_magistrat].[Ansættelse_mbu]
            WHERE 
                startdato <= getdate() 
                and slutdato > getdate()
                and Statuskode in ('1','3','5')
        ),
        til as (
            SELECT 
                [AnsættelsesID]
                ,[Institutionskode]
                ,[Tjenestenummer]
                ,[Startdato]
                ,[Slutdato]
                ,[Tillægsnummer]
                ,[Tillægsnavn]
            FROM
                [Personale].[sd_magistrat].[tillæg_mbu]
            WHERE
                Startdato <= getdate()
                and slutdato > getdate()
        ),
        joined as (
            SELECT 
                ans.AnsættelsesID
                ,ans.Institutionskode
                ,ans.Tjenestenummer
                ,ans.Stillingskode
                ,ans.Stilling
                ,ans.Overenskomst
                ,ans.Lønklasse
                ,ans.Afdeling
                ,til.Tillægsnummer
                ,til.Tillægsnavn
            FROM
                ans
                join til
                on ans.AnsættelsesID = til.AnsættelsesID
        )
        select 
            AnsættelsesID
            ,Institutionskode
            ,Tjenestenummer
            ,Stilling
            ,Stillingskode
            ,Afdeling
            ,CONCAT(Overenskomst,Lønklasse) as Lønklasse
            ,JSON_QUERY(
                '[' + STRING_AGG(
                    CONCAT('{"Tillægsnummer": "', Tillægsnummer, '", "Tillægsnavn": "', TRIM(Tillægsnavn), '"}'),
                    ','
                ) + ']'
            ) AS Tillaeg_List
        from joined
        group by
            AnsættelsesID
            ,Institutionskode
            ,Tjenestenummer
            ,Stilling
            ,Stillingskode
            ,Afdeling
            ,Overenskomst
            ,Lønklasse
        order by AnsættelsesID
    """
    items = get_items_from_query(conn_str, sql)
    items_df = pd.DataFrame(items)

    return items_df


# --------------------------------------------------
# Tekst og HTML til notifikationer
# --------------------------------------------------


def dk_month_relative(offset: int = 0) -> str:
    """Return Danish short month + last two digits of year based on relative offset.

    Example outputs:
        "feb 26", "dec 25".
    """
    # Danish short month names
    dk_months = [
        "jan",
        "feb",
        "mar",
        "apr",
        "maj",
        "jun",
        "jul",
        "aug",
        "sep",
        "okt",
        "nov",
        "dec",
    ]

    # Current date
    today = datetime.today()

    # Compute target year and month
    year = today.year + ((today.month - 1 + offset) // 12)
    month = (today.month - 1 + offset) % 12 + 1

    # Convert to Danish month name and 2‑digit year
    month_name = dk_months[month - 1]
    year_two_digits = str(year)[-2:]

    return f"{month_name} {year_two_digits}"


def html_table(rows: list, columns: list) -> str:
    """Formatér rækker som HTML-tabel.

    Args:
        rows: Rækker som dicts.
        columns: Kolonner som (overskrift, nøgle i rækken).
    """
    header = "".join(f"<th>{label}</th>" for label, _ in columns)
    body = "".join(
        "<tr>" + "".join(f"<td>{r.get(key)}</td>" for _, key in columns) + "</tr>"
        for r in rows
    )
    return (
        '<table border="1" cellpadding="4" style="border-collapse: collapse">'
        + f"<tr>{header}</tr>{body}</table>"
    )


def wage_rows_table(rows: list) -> str:
    """Formatér fejlende lønrækker som HTML-tabel med filnavn, linjenummer, indberetnings- og løndato."""
    return html_table(
        rows,
        [
            ("Filnavn", "File_name"),
            ("Linjenummer", "Line_no"),
            ("Indberetningsdato", "File date"),
            ("Løndato", "Wage date"),
        ],
    )


def employment_table(employments: list) -> str:
    """Formatér en liste af ansættelser som HTML-tabel."""
    if not employments:
        return "<p>Ingen ansættelser fundet.</p>"
    return html_table(
        employments,
        [
            ("Tjenestenummer", "Tjenestenummer"),
            ("Institutionskode", "Institutionskode"),
            ("Afdeling", "Afdeling"),
            ("Startdato", "Startdato"),
            ("Slutdato", "Slutdato"),
            ("Status for ansættelse", "StatusTekst"),
        ],
    )


def employment_list(employments: list) -> str:
    """Formatér en liste af ansættelser som HTML-lister."""
    return (
        "".join(
            "<ul>"
            + "".join(
                f"<li>{label}: {e.get(key)}</li>"
                for label, key in [
                    ("Tjenestenummer", "Tjenestenummer"),
                    ("Institutionskode", "Institutionskode"),
                    ("Afdeling", "Afdeling"),
                    ("Startdato", "Startdato"),
                    ("Slutdato", "Slutdato"),
                    ("Status for ansættelse", "StatusTekst"),
                ]
            )
            + "</ul>"
            for e in employments
        )
        or "<p>Ingen ansættelser fundet.</p>"
    )


def employment_status(statuses: list, date_label: str) -> str:
    """Formatér tjenestenummerets ansættelser for hver dato i statuses."""
    return "".join(
        f"<p>Tjenestenummerets ansættelsesstatus på {date_label} ({s.get('Dato')}):</p>"
        + employment_list(s.get("Ansættelser", []))
        for s in statuses
    )
