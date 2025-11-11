"""Module to handle item processing"""
# from mbu_rpa_core.exceptions import ProcessError, BusinessError

import logging

from helpers.config import WORKER_MAP
from helpers.process_constants import PROCESS_CONSTANTS

logger = logging.getLogger(__name__)


def process_item(item_data: dict, item_reference: str):
    """Function to handle item processing"""
    assert item_data, "Item data is required"
    assert item_reference, "Item reference is required"

    proc_args = PROCESS_CONSTANTS["kv_proc_args"]

    process_type = proc_args.get("process").upper()
    notification_type = proc_args.get("notification_type")
    # notification_receiver = proc_args.get("notification_receiver")  UNCOMMENT IN PROD

    # UNCOMMENT IN PROD
    # if notification_receiver == "AF":
    #     notification_receiver = data.get("AF_email")
    # UNCOMMENT IN PROD

    notification_receiver = "dadj@aarhus.dk"  # REMOVE IN PROD

    # Find and apply worker
    worker = WORKER_MAP.get(notification_type, None)
    if not worker:
        raise ValueError(f"No worker defined for process {notification_type}")

    worker(
        data=item_data,
        process_type=process_type,
        notification_receiver=notification_receiver
    )

    logger.info("Item processing finished")
