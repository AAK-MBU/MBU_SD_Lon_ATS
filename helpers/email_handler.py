"""Module to contain different workers"""

import json
import logging
import re

from helpers import smtp_util
from helpers.helper_functions import dk_month_relative, find_pair_info, get_tillaeg_navn
from helpers.process_constants import PROCESS_CONSTANTS
from helpers.tillaeg_pairs import tillaeg_pairs

logger = logging.getLogger(__name__)


def handle_email(data: dict, process_type: str, notification_receiver: str):
    """Function to send email to inputted receiver"""

    receiver = notification_receiver

    sender = PROCESS_CONSTANTS["e-mail_noreply"]

    email_subject, email_body = construct_worker_text(
        process_type=process_type, data=data
    )

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

    # Medarbejder tilkoblet forkert TRIO SKOLEKODE
    elif process_type == "KV5":
        error = data.get("Error")
        Folder_date = data.get("Folder_date")
        file_name = data.get("File_name")
        trio_school_code = data.get("Trio_school_code")

        if error == "NO_ACTIVE_XA_EMPLOYMENT":
            subject = "Ikke eksisterende medarbejder fundet i lønudtræk"

            # Construct message
            text = (
                "<h4>Følgende tjenestenummer er fundet i et lønudtræk, men eksisterer ikke i MBU's ansættelsesdata</h4>"
                + f"<p>Tjenestenummer: {person_id}</p>"
                + "<p>Tjenestenummeret blev fundet i et lønudtræk for skole med følgende skolekode:</p>"
                + f"<p>TRIO skolekode: {trio_school_code}</p>"
                + f"<p>Dato for lønudtrækket: {Folder_date}</p>"
                + f"<p>Filnavn: {file_name}</p>"
            )

        else:
            allowed_sd = data.get("Allowed_sd")

            subject = "Medarbejder er tilkoblet forkert TRIO skolekode"

            # Construct message
            text = (
                "<h4>Følgende medarbejder er tilkoblet den forkerte TRIO skolekode, på baggrund af den SD Afdelingskode, de står med i deres ansættelse</h4>"
                + f"<p>Tjenestenummer: {person_id}</p>"
                + f"<p>Navn: {person_name}</p>"
                + f"<p>Overenskomst: {overenskomst}</p>"
                + f"<p>SD institutionskode: {sd_inst_kode}</p>"
                + f"<p>SD Afdelingskode: {afdeling}</p>"
                + f"<p>TRIO skolekode: {trio_school_code}</p>"
                + f"<p>Følgende SD afdelingskoder er tilkoblet skolekode {trio_school_code}:</p>"
                + f"<p>{allowed_sd}:</p>"
            )

    elif process_type == "KV6":
        prev_beloeb = data.get("sum_tillægsbeløb_forrige", None)
        curr_beloeb = data.get("sum_tillægsbeløb_nu", None)
        change_beloeb = data.get("ændringer_beløb", None)
        prev_trin = data.get("sum_trin_forrige", None)
        curr_trin = data.get("sum_trin_nu", None)
        change_trin = data.get("ændringer_trin", None)

        curr_month_txt = f"({dk_month_relative(offset=0)})"
        prev_month_txt = f"({dk_month_relative(offset=-1)})"

        subject = "Ændring på lønsammensætning for medarbejder på gammel fleksordning"

        change_beloeb_txt = (
            "<p><i>Alle beløb er i 31/3-00 niveau</i></p>"
            + f"<p>Tidligere tillægsbeløb {prev_month_txt}: {prev_beloeb:,.2f}</p>".replace(
                ".", ";"
            )
            .replace(",", ".")
            .replace(";", ",")
            + f"<p>Nuværende tillægsbeløb {curr_month_txt}: {curr_beloeb:,.2f}</p>".replace(
                ".", ";"
            )
            .replace(",", ".")
            .replace(";", ",")
            + f"<p>Ændring i tillægsbeløb: {change_beloeb:,.2f}</p>".replace(".", ";")
            .replace(",", ".")
            .replace(";", ",")
            + "-" * 50
        )

        change_trin_txt = (
            f"<p>Tidligere løntrin {prev_month_txt}: {prev_trin}</p>"
            + f"<p>Nuværende løntrin {curr_month_txt}: {curr_trin}</p>"
            + f"<p>Ændring i løntrin: {change_trin}</p>"
            + "-" * 50
        )

        text = (
            "<h4>Følgende medarbejder er ansat på gammel fleksordning og har fået en ændring i sin lønsammensætning</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + "-" * 50
            + (f"{change_beloeb_txt}" if change_beloeb and change_beloeb != 0 else "")
            + (f"{change_trin_txt}" if change_trin and change_trin != 0 else "")
        )

    elif process_type == "KV7":
        current_tillaeg = data.get("available_tillaeg", None)
        required_tillaeg = data.get("required_tillaeg", None)
        missing_tillaeg = data.get("missing_tillaeg", None)
        loenklasse = data.get("Loenklasse", None)
        stilling = data.get("Stilling", None)
        enhedsnavn = data.get("enhnavn", None)
        current_tillaeg_dict = data.get("Tillaeg_List")
        subject = "Medarbejder mangler påkrævet tillæg fra skabelonansættelse"

        conn_str_faelles = PROCESS_CONSTANTS["FaellesDbConnectionString"]

        current_tillaeg_dict = json.loads(current_tillaeg_dict)
        current_tillaeg_dict = {
            item["Tillægsnummer"]: item["Tillægsnavn"]
            for item in current_tillaeg_dict
            if isinstance(item, dict)
            and "Tillægsnummer" in item
            and "Tillægsnavn" in item
        }

        missing_tillaeg_text = (
            "<ul>"
            + "".join(
                f"<li>{item} ({get_tillaeg_navn(conn_str_faelles, item)['Tillægsnavn']})</li>"
                for item in missing_tillaeg
            )
            + "</ul>"
        )

        current_tillaeg_text = (
            "<ul>"
            + "".join(
                f"<li>{item} ({current_tillaeg_dict[str(item)]})</li>"
                for item in current_tillaeg
            )
            + "</ul>"
        )

        required_tillaeg_text = (
            "<ul>"
            + "".join(
                f"<li>{item} ({get_tillaeg_navn(conn_str_faelles, item)['Tillægsnavn']})</li>"
                for item in required_tillaeg
            )
            + "</ul>"
        )

        text = (
            "<h4>Følgende medarbejder mangler et påkrævet tillæg</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + f"<p>Stilling: {stilling}</p>"
            + f"<p>SD institutionskode: {sd_inst_kode}</p>"
            + f"<p>Enhed: {enhedsnavn}</p>"
            + f"<p>Lønklasse: {loenklasse}</p>"
            + "-" * 50
            + "<p>Manglende tillæg:</p>"
            + missing_tillaeg_text
            + "-" * 50
            + "<p>Medarbejderen har følgende tillæg:</p>"
            + current_tillaeg_text
            + "<p>Medarbejderens lønklasse, enhed, institutionskode og stilling kræver følgende tillæg:</p>"
            + required_tillaeg_text
        )

    return subject, text
