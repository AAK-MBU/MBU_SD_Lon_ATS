"""Mailtekst til KV7: manglende påkrævet tillæg"""

import json

from helpers.proces_helpers import get_tillaeg_navn
from helpers.process_constants import PROCESS_CONSTANTS


def construct_email(data: dict) -> tuple[str, str]:
    """Konstruerer emne og HTML-tekst til mailen for KV7"""

    person_id = data.get("Tjenestenummer", None)
    sd_inst_kode = data.get("Institutionskode", None)

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
