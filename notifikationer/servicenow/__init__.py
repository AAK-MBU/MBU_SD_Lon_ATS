"""Notifikation ved oprettelse af sager i ServiceNow gennem API'et (under udarbejdelse)"""

# Kontrol-specifikke sagsbyggere: {"KV1": kv1.construct_case, ...}.
# Hver construct_case(data) returnerer sagens felter som dict.
# Kontroller uden sagsbygger får sagens tekst fra mailteksten.
CASE_BUILDERS = {}
