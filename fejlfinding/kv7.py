"""KV7: finder medarbejdere der mangler påkrævede minimumstillæg"""

from helpers import ats_helpers, proces_helpers
from helpers.process_constants import PROCESS_CONSTANTS


def kv7(exclude_schoolname: list, exclude_dagtilbudname: list):
    """
    Function to check that employees have necesarry tillæg
    """

    conn_str_faellesdb = PROCESS_CONSTANTS["FaellesDbConnectionString"]
    conn_str_mbu = PROCESS_CONSTANTS["DBCONNECTIONSTRINGPROD"]
    # Get employees
    employees_df = proces_helpers.get_employees(conn_str_faellesdb)

    # Select almen
    almen_employees_df = proces_helpers.select_employees_almen(
        conn_str_mbu,
        conn_str_faellesdb,
        exclude_schoolname,
        exclude_dagtilbudname,
        employees_df,
    )

    # Select only school employees for now
    almen_employees_df = almen_employees_df[almen_employees_df["Enhedstype"] == "Skole"]

    # Get mapping of tillæg and lønklasser
    minimumstillaeg_df = proces_helpers.get_minimumstillaeg(conn_str_mbu)

    # Compare
    employee_missing_tillaeg = proces_helpers.check_employee_tillaeg(
        employee_df=almen_employees_df, minimumstillaeg_df=minimumstillaeg_df
    )

    items = ats_helpers.item_df_to_item_list(employee_missing_tillaeg)

    return items
