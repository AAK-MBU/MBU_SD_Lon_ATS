"""Module to contain different workers"""

import logging

import re

from helpers import smtp_util
from helpers.helper_functions import (
    find_pair_info,
    # find_match_ovk
)
from helpers.tillaeg_pairs import tillaeg_pairs
from helpers.process_constants import PROCESS_CONSTANTS

logger = logging.getLogger(__name__)


def handle_email(
    data: dict,
    process_type: str,
    notification_receiver: str,
):
    """Function to send email to inputted receiver"""

    receiver = notification_receiver

    sender = PROCESS_CONSTANTS["e-mail_noreply"]

    email_subject, email_body = construct_worker_text(
        process_type=process_type,
        data=data
    )

    # REMOVE IN PROD
    print(f"receiver: {receiver}")
    if receiver != "dadj@aarhus.dk":
        logger.info("receiver IS NOT CORRECT !!!")
        import sys
        sys.exit()
    # REMOVE IN PROD

    smtp_util.send_email(
        receiver="dadj@aarhus.dk",  # REMOVE IN PROD
        # receiver=receiver,  # UNCOMMENT IN PROD
        sender=sender,
        subject=email_subject,
        body=email_body,
        smtp_server=PROCESS_CONSTANTS["smtp_server"],
        smtp_port=PROCESS_CONSTANTS["smtp_port"],
        html_body=True,
    )

    logger.info(f"E-mail sent to {receiver}")


def construct_worker_text(process_type: str, data: dict):
    """Function to construct text for the different processes"""

    subject = ""
    text = ""

    person_id = data.get("Tjenestenummer", None)
    person_name = data.get("Navn", None)
    overenskomst = data.get("Overenskomst", None)
    afdeling = data.get("Afdeling", None)
    sd_inst_kode = data.get("Institutionskode", None)
    enhedsnavn = data.get("Enhedsnavn", None)

    # Inspirationsansættelser
    if process_type == "KV1":
        instruction_link = "https://intranet.aarhuskommune.dk/documents/146889"

        subject = "Inspirationsansættelse på XA institution"

        text = (
            "<p>Der er ved en fejl blevet oprettet en inspirationsansættelse på en XA enhed i jeres Administrative Fællesskab. <br>"
            + "Inspirationsansættelser skal udelukkende oprettes på XC enheder. <br>"
            + "Du skal derfor slette ansættelsen på XA enheden og oprette ansættelsen på ny på den korrekte XC enhed"
            + "- se nedenstående.</p>"
            + "-" * 100
            + "<h4>Følgende inspirationsansættelse er registreret på en XA SD-institutionskode:</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + f"<p>Afdeling: {afdeling}</p>"
            + f"<p>SD institutionskode: {sd_inst_kode}</p>"
            + f"<p>Registreret overenskomst: {overenskomst}</p>"
            + f'<p>Du kan finde vejledningen til "Inspirationsansættelser" på <a href={instruction_link}>dette link</a> (AARHUSINTRA)</p>'
            + "-" * 100
        )

    # Manglende tillægsnummer
    elif process_type == "KV2":
        # Initialize found pair
        found_number = int(data["Tillægsnummer"])
        found_name = data["Tillægsnavn"]
        found_type = re.search(pattern=r"([A|B])-(?!.*-)", string=found_name).group(1)

        # Initialize supposed match
        match_number = None
        match_name = None
        match_type = None
        # Find supposed match
        for pair in tillaeg_pairs:
            match_set = find_pair_info(pair, found_number)
            if match_set:
                match_number, match_name = match_set
                match_type = re.search(
                    pattern=r"([A|B])-(?!.*-)", string=match_name
                ).group(1)

        subject = "Manglende tillægsnummer i ansættelse"

        # Construct message
        text = (
            "<h4>Følgende ansættelse mangler et tillægsnummer, "
            + f"da denne er registreret med et {found_type}-tillægsnummer, men mangler et {match_type}-tillægsnummer:</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + f"<p>Navn: {person_name}</p>"
            + f"<p>Overenskomst: {overenskomst}</p>"
            + f"<p>Afdeling: {afdeling} ({enhedsnavn})</p>"
            + f"<p>SD institutionskode: {sd_inst_kode}</p>"
            + f"<p>Fundet tillæg: {found_number}-{found_name}</p>"
            + f"<p>Manglende tillæg: {match_number}-{match_name}</p>"
            + "Ved rettelse af denne fejl skal lønsammensætningen kontrolleres. Ved spørgsmål, kontakt da Personale."
        )

    # Forkert overenskomst skole/dagtilbud
    elif process_type in ("KV3", "KV3-DEV"):

        afdtype_txt = data["afdtype_txt"]
        # exp_ovk = find_match_ovk(overenskomst)

        subject = "Fejl i SD-overenskomst"

        text = (
            "<h4>Følgende ansættelse er oprettet med en forkert SD overenskomst:</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + f"<p>Navn: {person_name}</p>"
            + f"<p>Afdeling: {afdeling} ({enhedsnavn})</p>"
            + f"<p>Afdelingstype: {afdtype_txt}</p>"
            + f"<p>SD institutionskode: {sd_inst_kode}</p>"
            + f"<p>Registreret overenskomst: {overenskomst}</p>"
            # + f"<p>Forventet overenskomst: {exp_ovk}</p>"
        )

    # Manglende låst anciennitetsdato
    elif process_type == "KV4":
        instruction_link = "https://aarhuskommune.sharepoint.com/sites/IntranetDocumentSite/Intranetdokumentbibliotek/Forms/Seneste%20dokumenter.aspx?id=%2Fsites%2FIntranetDocumentSite%2FIntranetdokumentbibliotek%2FL%C3%A5st%20p%C3%A5%20grundl%C3%B8nstrin%2Epdf&parent=%2Fsites%2FIntranetDocumentSite%2FIntranetdokumentbibliotek&p=true&ga=1"

        subject = "Manglende låst anciennitet på leder"

        text = (
            "<h4>Følgende leder har ikke fået fastlåst sin anciennitetsdato til dato 31.12.9999:</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + f"<p>Navn: {person_name}</p>"
            + f"<p>Afdeling: {afdeling}</p>"
            + f"<p>SD institutionskode: {sd_inst_kode}</p>"
            + f"<p>Registreret overenskomst: {overenskomst}</p>"
            + f"<p>Du kan finde vejledningen <a href={instruction_link}>her</a>"
            + "<p>Bliver datoen ikke rettet til 31.12.9999, vil lederens grundlønstrin stige med et løntrin hvert år. <br>"
            + "OBS hvis der er tilknyttet et grundlønstillæg til stillingen skal du huske at oprette dette manuelt (måske er tillægget allerede oprettet, se lønsammensætning)."
        )

    return subject, text
