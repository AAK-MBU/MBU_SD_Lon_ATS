"""Mailtekst til KV5: fejl i TRIO-lønudtræk"""

from helpers.proces_helpers import employment_status, employment_table, wage_rows_table


def construct_email(data: dict) -> tuple[str, str]:
    """Konstruerer emne og HTML-tekst til mailen for KV5"""

    person_id = data.get("Tjenestenummer", None)
    person_name = data.get("Navn", None)
    overenskomst = data.get("Overenskomst", None)
    afdeling = data.get("Afdeling", None)
    sd_inst_kode = data.get("Institutionskode", None)

    # Medarbejder tilkoblet forkert TRIO SKOLEKODE
    error = data.get("Error")
    records = data.get("Records", [])
    trio_school_code = data.get("Trio_school_code")
    allowed_sd = data.get("Allowed_sd", "")

    trio_messages = {
        "NO_EMPLOYMENT": {
            "subject": "TRIO: Ikke eksisterende medarbejder fundet i lønudtræk",
            "text": (
                "<h4>Følgende tjenestenummer er fundet i et lønudtræk, men eksisterer ikke i MBU's ansættelsesdata</h4>"
                + f"<p>Tjenestenummer: {person_id}</p>"
                + "<p>Lønrækker med dette tjenestenummer:</p>"
                + wage_rows_table(records)
            ),
        },
        "SD_NOT_VALID_FOR_TRIO": {
            "subject": "TRIO: Medarbejder er tilkoblet forkert TRIO skolekode",
            "text": (
                "<h4>Følgende medarbejder er tilkoblet den forkerte TRIO skolekode, på baggrund af den SD Afdelingskode, de står med i deres ansættelse</h4>"
                + f"<p>Tjenestenummer: {person_id}</p>"
                + f"<p>Navn: {person_name}</p>"
                + f"<p>Overenskomst: {overenskomst}</p>"
                + f"<p>SD institutionskode: {sd_inst_kode}</p>"
                + f"<p>SD Afdelingskode: {afdeling}</p>"
                + f"<p>TRIO skolekode: {trio_school_code}</p>"
                + f"<p>Følgende SD afdelingskoder er tilkoblet skolekode {trio_school_code}:</p>"
                + f"<p>{allowed_sd}:</p>"
            ),
        },
        "EMPLOYMENT_NOT_ACTIVE_ON_WAGE": {
            "subject": "TRIO: Løn registreret på ansættelse inaktiv på løndatoen",
            "text": (
                "<h4>Der er på følgende tjenestenummer indberettet løn, hvor ansættelsen ikke var aktiv på løndatoen</h4>"
                + f"<p>Tjenestenummer: {person_id}</p>"
                + f"<p>Navn: {person_name}</p>"
                + "<p>Lønrækker med fejl:</p>"
                + wage_rows_table(records)
                + employment_status(data.get("wage_status", []), "løndatoen")
                + "<p>Alle ansættelser på det CPR-nr. der senest var tilknyttet tjenestenummeret:</p>"
                + employment_table(data.get("cpr_employments", []))
            ),
        },
        "EMPLOYMENT_NOT_ACTIVE_ON_RECORD_ACTIVE_ON_WAGE": {
            "subject": "TRIO: Løn registreret på ansættelse inaktiv på indberetningsdatoen",
            "text": (
                "<h4>Der er på følgende tjenestenummer indberettet løn, hvor ansættelsen ikke var aktiv på indberetningsdatoen</h4>"
                + f"<p>Tjenestenummer: {person_id}</p>"
                + f"<p>Navn: {person_name}</p>"
                + "<p>Lønrækker med fejl:</p>"
                + wage_rows_table(records)
                + employment_status(
                    data.get("record_status", []), "indberetningsdatoen"
                )
                + employment_status(data.get("wage_status", []), "løndatoen")
            ),
        },
    }

    msg = trio_messages[error]
    subject = msg["subject"]
    text = msg["text"]

    return subject, text
