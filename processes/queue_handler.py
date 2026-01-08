"""Module to hande queue population"""

import asyncio
import json
import logging
import sys

import os
from pathlib import Path

from datetime import datetime

from automation_server_client import Workqueue

from helpers import config
from helpers import helper_functions
from helpers.process_constants import PROCESS_CONSTANTS

logger = logging.getLogger(__name__)


def retrieve_items_for_queue() -> list[dict]:
    """Function to populate queue"""
    data = []
    references = []

    proc_args = ""

    proc_args = PROCESS_CONSTANTS["kv_proc_args"]

    logger.info(f"process arguments: {proc_args}")

    process = proc_args.get("process", None).upper()

    if not process or process == "":
        raise ValueError("No process defined in sys arguments!")

    # Set variables for function call
    process_procedure = config.PROCESS_PROCEDURE_DICT.get(
        process,
        None
    )
    if not process_procedure:
        raise ValueError(f"Process procedure for {process} not defined in dictionary")

    control_procedure = process_procedure.get(
        "procedure",
        ValueError(f"No stored procedure for {process_procedure} in dictionary")
    )
    procedure_params = process_procedure.get(
        "parameters",
        ValueError(f"No parameters for {process_procedure} in dictionary")
    )

    logger.info(f"Running {process = }, procedure {control_procedure.__name__}, {procedure_params = }")

    # Get items for process
    retrieved_items = control_procedure(**procedure_params)

    if retrieved_items:
        for i, item in enumerate(retrieved_items):
            if "--kv5" in sys.argv:
                reference = f"{process}_{datetime.now().strftime('%d%m%y')}_{item.get('Tjenestenummer')}"
                references.append(reference)

            else:
                references.append(f"{process}_{datetime.now().strftime('%d%m%y')}_{i+1}")

            formatted_item = helper_functions.format_item(item)
            data.append(formatted_item)

        logger.info(f"Populated queue with {len(retrieved_items)} items.")

    else:
        logger.info("No items found. Queue not populated")

    items = [
        {"reference": ref, "data": d} for ref, d in zip(references, data, strict=True)
    ]

    return items


def create_sort_key(item: dict) -> str:
    """
    Create a sort key based on the entire JSON structure.
    Converts the item to a sorted JSON string for consistent ordering.
    """
    return json.dumps(item, sort_keys=True, ensure_ascii=False)


async def concurrent_add(workqueue: Workqueue, items: list[dict]) -> None:
    """
    Populate the workqueue with items to be processed.
    Uses concurrency and retries with exponential backoff.

    Args:
        workqueue (Workqueue): The workqueue to populate.
        items (list[dict]): List of items to add to the queue.

    Returns:
        None

    Raises:
        Exception: If adding an item fails after all retries.
    """
    sem = asyncio.Semaphore(config.MAX_CONCURRENCY)

    async def add_one(it: dict):
        reference = str(it.get("reference") or "")
        data = {"item": it}

        async with sem:
            for attempt in range(1, config.MAX_RETRIES + 1):
                try:
                    await asyncio.to_thread(workqueue.add_item, data, reference)
                    logger.info("Added item to queue with reference: %s", reference)
                    return True

                except Exception as e:
                    if attempt >= config.MAX_RETRIES:
                        logger.error(
                            "Failed to add item %s after %d attempts: %s",
                            reference,
                            attempt,
                            e,
                        )
                        return False

                    backoff = config.RETRY_BASE_DELAY * (2 ** (attempt - 1))

                    logger.warning(
                        "Error adding %s (attempt %d/%d). Retrying in %.2fs... %s",
                        reference,
                        attempt,
                        config.MAX_RETRIES,
                        backoff,
                        e,
                    )
                    await asyncio.sleep(backoff)

    if not items:
        logger.info("No new items to add.")
        return

    sorted_items = sorted(items, key=create_sort_key)
    logger.info(
        "Processing %d items sorted by complete JSON structure", len(sorted_items)
    )

    results = await asyncio.gather(*(add_one(i) for i in sorted_items))
    successes = sum(1 for r in results if r)
    failures = len(results) - successes

    logger.info(
        "Summary: %d succeeded, %d failed out of %d", successes, failures, len(results)
    )
