"""Mailtekst til KV4: manglende låst anciennitet på leder"""


def construct_email(data: dict) -> tuple[str, str]:
    """Konstruerer emne og HTML-tekst til mailen for KV4"""

    person_id = data.get("Tjenestenummer", None)
    person_name = data.get("Navn", None)
    overenskomst = data.get("Overenskomst", None)
    afdeling = data.get("Afdeling", None)
    sd_inst_kode = data.get("Institutionskode", None)

    # Manglende låst anciennitetsdato
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
