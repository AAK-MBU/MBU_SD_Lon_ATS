"""Hjælpefunktioner til Automation Server: kø-elementer, API-kald og logging"""

import logging
import os
from datetime import date
from decimal import Decimal

import pandas as pd
import requests
from automation_server_client import WorkItem, Workqueue
from dotenv import load_dotenv


def get_workqueue_items(workqueue: Workqueue, return_data=False):
    """
    Retrieve items from the specified workqueue.
    If the queue is empty, return an empty list.
    """
    load_dotenv()

    url = os.getenv("ATS_URL")
    token = os.getenv("ATS_TOKEN")

    if not url or not token:
        raise OSError("ATS_URL or ATS_TOKEN is not set in the environment")

    headers = {"Authorization": f"Bearer {token}"}

    workqueue_items = {} if return_data else set()

    page = 1
    size = 200  # max allowed

    while True:
        full_url = f"{url}/workqueues/{workqueue.id}/items?page={page}&size={size}"
        response = requests.get(full_url, headers=headers, timeout=60)
        response.raise_for_status()

        res_json = response.json().get("items", [])

        if not res_json:
            break

        for row in res_json:
            ref = row.get("reference")
            if ref:
                if return_data:
                    workqueue_items[ref] = row
                else:
                    workqueue_items.add(ref)

        page += 1

    return workqueue_items


def get_item_info(item: WorkItem):
    """Unpack item"""
    return item.data["item"]["data"], item.data["item"]["reference"]


def init_logger():
    """Initialize the root logger with JSON formatting."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(module)s.%(funcName)s:%(lineno)d — %(message)s",
        datefmt="%H:%M:%S",
    )


def value_formatter(value):
    """Formatér en værdi rekursivt, så den kan serialiseres til JSON."""
    if isinstance(value, dict):
        return {key: value_formatter(v) for key, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [value_formatter(v) for v in value]
    if isinstance(value, date):
        return value.strftime("%d-%m-%Y")
    if isinstance(value, Decimal):
        return int(value)
    return value


def format_item(item: dict):
    """Format dates in dict, e.g. for json parsing"""

    return {key: value_formatter(value) for key, value in item.items()}


def item_df_to_item_list(item_df: pd.DataFrame) -> list:
    items = list(item_df.T.to_dict().values())

    return items
