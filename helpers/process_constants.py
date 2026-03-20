"""
Module to contain and lazy-load constants
"""

import json
import os
import sys

from mbu_dev_shared_components.database.connection import RPAConnection

_cached_constants = {}


def _load_constants():
    if _cached_constants:  # already loaded, do nothing
        return

    with RPAConnection(db_env="PROD", commit=False) as conn:
        _cached_constants["FaellesDbConnectionString"] = os.getenv(
            "DBCONNECTIONSTRINGFAELLESSQL"
        )

        _cached_constants["DBCONNECTIONSTRINGPROD"] = os.getenv(
            "DBCONNECTIONSTRINGPROD"
        )

        # Determine which --kvX flag is present
        kv_flag = next((arg for arg in sys.argv if arg.startswith("--kv")), None)

        if kv_flag:
            key = f"sdloen_{kv_flag[2:]}_procargs"  # "--kv3" -> "sdloen_kv3_procargs"
            value = conn.get_constant(key).get("value", "{}")
            _cached_constants["kv_proc_args"] = json.loads(value)

        _cached_constants["e-mail_noreply"] = conn.get_constant("e-mail_noreply").get(
            "value", ""
        )

        _cached_constants["smtp_server"] = conn.get_constant("smtp_adm_server").get(
            "value", ""
        )
        _cached_constants["smtp_port"] = conn.get_constant("smtp_port").get("value", "")


class Constants(dict):
    """
    Class to contain an allow easy access to both global and flow-specific constants
    """

    def __getitem__(self, key):
        _load_constants()

        return _cached_constants[key]


PROCESS_CONSTANTS = Constants()
