"""Afsendelse af mailnotifikationer"""

import logging

from helpers.process_constants import PROCESS_CONSTANTS
from notifikationer.mail import MAIL_BUILDERS, smtp_util

logger = logging.getLogger(__name__)


def handle_email(data: dict, process_type: str, notification_receiver: str):
    """Function to send email to inputted receiver"""

    receiver = notification_receiver

    sender = PROCESS_CONSTANTS["e-mail_noreply"]

    email_subject, email_body = MAIL_BUILDERS[process_type](data)

    smtp_util.send_email(
        receiver=receiver,
        sender=sender,
        subject=email_subject,
        body=email_body,
        smtp_server=PROCESS_CONSTANTS["smtp_server"],
        smtp_port=PROCESS_CONSTANTS["smtp_port"],
        html_body=True,
    )

    logger.info(f"E-mail sent to {receiver}")
