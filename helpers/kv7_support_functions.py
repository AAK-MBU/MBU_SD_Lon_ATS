import json

import pandas as pd

from helpers import helper_functions


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
    items = helper_functions.get_items_from_query(conn_str, sql)
    items_df = pd.DataFrame(items)

    return items_df
