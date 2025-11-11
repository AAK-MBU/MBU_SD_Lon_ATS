"""
Module to contain and lazy-load constants
"""

import os
import sys
import json

from mbu_dev_shared_components.database.connection import RPAConnection

_cached_constants = {}


def _load_constants():
    if _cached_constants:  # already loaded, do nothing
        return

    with RPAConnection(db_env="PROD", commit=False) as conn:
        _cached_constants["FaellesDbConnectionString"] = os.getenv("DBCONNECTIONSTRINGFAELLESSQL")

        _cached_constants["DBCONNECTIONSTRINGPROD"] = os.getenv("DBCONNECTIONSTRINGPROD")

        if "--kv1" in sys.argv:
            _cached_constants["kv_proc_args"] = json.loads(conn.get_constant("sdloen_kv1_procargs").get("value", "{}"))

        elif "--kv3" in sys.argv:
            _cached_constants["kv_proc_args"] = json.loads(conn.get_constant("sdloen_kv3_procargs").get("value", "{}"))

        elif "--kv4" in sys.argv:
            _cached_constants["kv_proc_args"] = json.loads(conn.get_constant("sdloen_kv4_procargs").get("value", "{}"))

        _cached_constants["e-mail_noreply"] = conn.get_constant("e-mail_noreply").get("value", "")

        _cached_constants["smtp_server"] = conn.get_constant("smtp_adm_server").get("value", "")
        _cached_constants["smtp_port"] = conn.get_constant("smtp_port").get("value", "")

class Constants(dict):
    """
    Class to contain an allow easy access to both global and flow-specific constants
    """

    def __getitem__(self, key):
        _load_constants()

        return _cached_constants[key]


PROCESS_CONSTANTS = Constants()
