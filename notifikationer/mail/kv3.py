"""Mailtekst til KV3 og KV3-DEV: forkert SD-overenskomst"""


def construct_email(data: dict) -> tuple[str, str]:
    """Konstruerer emne og HTML-tekst til mailen for KV3 og KV3-DEV"""

    person_id = data.get("Tjenestenummer", None)
    person_name = data.get("Navn", None)
    overenskomst = data.get("Overenskomst", None)
    afdeling = data.get("Afdeling", None)
    sd_inst_kode = data.get("Institutionskode", None)
    enhedsnavn = data.get("Enhedsnavn", None)

    # Forkert overenskomst skole/dagtilbud
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

    return subject, text
