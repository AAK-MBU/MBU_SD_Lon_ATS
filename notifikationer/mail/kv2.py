"""Mailtekst til KV2: manglende tillægsnummer i tillægspar"""

import re

from helpers.proces_helpers import find_pair_info
from helpers.tillaeg_pairs import tillaeg_pairs


def construct_email(data: dict) -> tuple[str, str]:
    """Konstruerer emne og HTML-tekst til mailen for KV2"""

    person_id = data.get("Tjenestenummer", None)
    person_name = data.get("Navn", None)
    overenskomst = data.get("Overenskomst", None)
    afdeling = data.get("Afdeling", None)
    sd_inst_kode = data.get("Institutionskode", None)
    enhedsnavn = data.get("Enhedsnavn", None)

    # Manglende tillægsnummer
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

    return subject, text
