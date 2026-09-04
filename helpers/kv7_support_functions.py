import json
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from helpers import helper_functions

# Tillæg columns from get_employees() and the key each gets in the JSON output
TILLAEG_JSON_KEYS = {
    "Tillægsnummer": "Tillægsnummer",
    "Tillægsnavn": "Tillægsnavn",
    "TillægStart": "Startdato",
    "TillægSlut": "Slutdato",
}


def check_employee_tillaeg(
    employee_df: pd.DataFrame,
    # dev_employee_df: pd.DataFrame,
    minimumstillaeg_df: pd.DataFrame,
):
    """
    Check that every employee has the required tillæg
    """

    # Only tillæg that are active today count as present
    active_df = select_active_tillaeg(
        employee_df, start_col="TillægStart", end_col="TillægSlut"
    )

    # One row per employee with their active tillæg gathered in Tillaeg_List
    employee_df = collect_tillaeg(active_df, json_cols=["Tillægsnummer", "Tillægsnavn"])

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


def format_tillaeg_value(value):
    """Convert a single tillæg value to something JSON serializable."""

    if value is None or (not isinstance(value, (list, dict, set)) and pd.isna(value)):
        return None
    if isinstance(value, Decimal):
        value = int(value)
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d")
    return str(value).strip()


def tillaeg_is_active(start, end, on_date: pd.Timestamp) -> bool:
    """Check whether a tillæg is active on the given date (start <= date < end).

    Missing or out-of-range dates (e.g. slutdato 9999-12-31) count as open ended.
    """

    start = pd.to_datetime(start, errors="coerce")
    end = pd.to_datetime(end, errors="coerce")

    started = pd.isna(start) or start <= on_date
    not_ended = pd.isna(end) or end > on_date

    return started and not_ended


def select_active_tillaeg(
    df: pd.DataFrame, start_col: str, end_col: str, on_date: pd.Timestamp | None = None
) -> pd.DataFrame:
    """Keep only the rows whose tillæg is active on on_date, today by default.

    Employees are only present in the data through their tillæg rows, so an
    employee whose tillæg have all expired drops out of the selection entirely.
    """

    df = df.copy()

    if on_date is None:
        on_date = pd.Timestamp.today().normalize()

    is_active = df.apply(
        lambda row: tillaeg_is_active(row.get(start_col), row.get(end_col), on_date),
        axis=1,
    )

    df = df.loc[is_active, :]

    return df


def collect_tillaeg(
    df: pd.DataFrame,
    json_cols: list,
    list_col: str = "Tillaeg_List",
    group_cols: list | None = None,
) -> pd.DataFrame:
    """Gather the one-row-per-tillæg output from get_employees() per employee.

    Nothing is filtered on date here - pass the rows through
    select_active_tillaeg() first when only active tillæg should count.

    Arguments:
        df (pd.DataFrame): Rows from get_employees(), one per tillæg.
        json_cols (list): Tillæg columns to include in each JSON object.
        list_col (str): Name of the resulting JSON column.
        group_cols (list | None): Columns identifying an employee. Defaults to
            every column that is not a tillæg column.

    Returns:
        pd.DataFrame: One row per employee, where the tillæg columns are replaced
            by list_col: a JSON string with one object per tillæg, ordered by
            startdato. Employees with no tillæg get an empty list.
    """

    df = df.copy()

    tillaeg_cols = [col for col in TILLAEG_JSON_KEYS if col in df.columns]
    json_cols = [col for col in json_cols if col in df.columns]

    if group_cols is None:
        group_cols = [col for col in df.columns if col not in tillaeg_cols]

    def build_tillaeg(row):
        """Build one JSON entry, or None if the row has no tillæg."""
        if format_tillaeg_value(row["Tillægsnummer"]) is None:
            return None
        return {
            TILLAEG_JSON_KEYS[col]: format_tillaeg_value(row[col]) for col in json_cols
        }

    # Sort by startdato so the tillæg are listed chronologically per employee
    if "TillægStart" in df.columns:
        df["tillaeg_start"] = pd.to_datetime(df["TillægStart"], errors="coerce")
        df = df.sort_values("tillaeg_start", na_position="first")

    df["tillaeg_item"] = df.apply(build_tillaeg, axis=1)

    # dropna=False keeps employees with empty values in e.g. Lønklasse
    grouped = (
        df.groupby(group_cols, dropna=False, sort=False)["tillaeg_item"]
        .apply(lambda items: [item for item in items if item is not None])
        .reset_index()
        .rename(columns={"tillaeg_item": list_col})
    )

    grouped[list_col] = grouped[list_col].apply(
        lambda items: json.dumps(items, ensure_ascii=False)
    )

    return grouped


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

    items = helper_functions.get_items_from_query(conn_str, sql)
    items_df = pd.DataFrame(items)

    items_df["Stilling"] = items_df["Stilling"].replace(
        "Pædagmedhjælper", "Pædagogmedhjælper"
    )

    return items_df


def get_schools(connection_string_mbu: str, connection_string_faelles: str):
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
            "Tillægsnummer",
            "Tillægsnavn",
            "TillægStart",
            "TillægSlut",
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

    items = helper_functions.get_items_from_query(conn_str_mbu, sql)
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
                ,til.Startdato as TillægStart
                ,til.Slutdato as TillægSlut
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
            ,Tillægsnummer
            ,Tillægsnavn
            ,TillægStart
            ,TillægSlut
        from joined
        order by AnsættelsesID
    """
    items = helper_functions.get_items_from_query(conn_str, sql)
    items_df = pd.DataFrame(items)

    return items_df
