"""Mailtekst til KV1: inspirationsansættelser på XA-enheder"""


def construct_email(data: dict) -> tuple[str, str]:
    """Konstruerer emne og HTML-tekst til mailen for KV1"""

    person_id = data.get("Tjenestenummer", None)
    overenskomst = data.get("Overenskomst", None)
    afdeling = data.get("Afdeling", None)
    sd_inst_kode = data.get("Institutionskode", None)

    # Inspirationsansættelser
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

    return subject, text
