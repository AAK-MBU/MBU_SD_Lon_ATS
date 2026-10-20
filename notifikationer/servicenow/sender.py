"""Oprettelse af ServiceNow-sager som notifikation (under udarbejdelse)"""

import logging

from notifikationer.mail import MAIL_BUILDERS
from notifikationer.servicenow import CASE_BUILDERS
from notifikationer.servicenow.client import ServiceNowClient

logger = logging.getLogger(__name__)


def construct_case(data: dict, process_type: str, notification_receiver: str) -> dict:
    """Bygger sagens felter for kø-elementet.

    Bruger kontrollens sagsbygger i CASE_BUILDERS, hvis den findes, og ellers
    kontrollens mailtekst som sagens overskrift og beskrivelse.

    Args:
        data: Kø-elementets data fra kvalitetskontrollen.
        process_type: Kvalitetskontrollen, fx "KV1".
        notification_receiver: Modtageren af sagen, fx en tildelingsgruppe.

    Returns:
        Sagens felter som dict.
    """
    if process_type in CASE_BUILDERS:
        payload = CASE_BUILDERS[process_type](data)
    else:
        subject, body = MAIL_BUILDERS[process_type](data)
        payload = {"short_description": subject, "description": body}

    # TODO: Afklar hvilket felt modtageren skal sættes i
    payload["assignment_group"] = notification_receiver

    return payload


def handle_servicenow(data: dict, process_type: str, notification_receiver: str):
    """Opretter en ServiceNow-sag for kø-elementet"""

    payload = construct_case(data, process_type, notification_receiver)

    # TODO: Hent URL og credentials fra PROCESS_CONSTANTS, når nøglenavnene er fastlagt
    client = ServiceNowClient(base_url="", username="", password="")
    case_number = client.create_case(payload)

    logger.info(f"ServiceNow-sag {case_number} oprettet til {notification_receiver}")
