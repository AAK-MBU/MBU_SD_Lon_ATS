"""Mailtekst til KV6: ændret lønsammensætning på gammel fleksordning"""

from helpers.proces_helpers import dk_month_relative


def construct_email(data: dict) -> tuple[str, str]:
    """Konstruerer emne og HTML-tekst til mailen for KV6"""

    person_id = data.get("Tjenestenummer", None)

    prev_beloeb = data.get("sum_tillægsbeløb_forrige", None)
    curr_beloeb = data.get("sum_tillægsbeløb_nu", None)
    change_beloeb = data.get("ændringer_beløb", None)
    prev_trin = data.get("sum_trin_forrige", None)
    curr_trin = data.get("sum_trin_nu", None)
    change_trin = data.get("ændringer_trin", None)
    per_gruppe = data.get("Personale_gruppe", None)

    curr_month_txt = f"({dk_month_relative(offset=0)})"
    prev_month_txt = f"({dk_month_relative(offset=-1)})"

    subject = f"Ændring på lønsammensætning for medarbejder på gammel fleksordning - {per_gruppe}"

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
        f"<h4>Følgende medarbejder er ansat på gammel fleksordning i {per_gruppe} og har fået en ændring i sin lønsammensætning</h4>"
        + f"<p>Tjenestenummer: {person_id}</p>"
        + "-" * 50
        + (f"{change_beloeb_txt}" if change_beloeb and change_beloeb != 0 else "")
        + (f"{change_trin_txt}" if change_trin and change_trin != 0 else "")
    )

    return subject, text
