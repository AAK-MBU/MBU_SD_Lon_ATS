# Robot til at fremfinde og notificere om fejl i ansættelser i SD Løn

Dette projekt fokuserer på at fremfinde ansættelser der er registreret med fejl, fx i overenskomster eller løntillæg. Herefter notificerer robotten relevante modtagere gennem mails eller ServiceNow sager

## Kvalitetskontroller 
Robotten foretager følgende kvalitetskontroller af igangværende ansættelser

1. **Inspirationsansættelser ikke på institutionskode "XC"** <br>
    Inspirationsansættelser skal altid være tilknyttet en XC institutionskode. Inspirationsansættelser foregår på overenskomsten 47302. Processen tjekker om der er nogle aktive ansættelser på overenskomst 47302 som ikke er på XC institutionskode. 

2. **Mangler A/B forhåndsaftale** <br>
    En række forhåndsaftaler (med løntillæg) er defineret i par, og skal altid fremgå i par. Dvs. at man ikke kan have en A-aftale uden også at skulle have en B-aftale og omvendt. Disse par er prædefinerede. Processen tjekker om nogle aktive ansættelser har en aftale fra disse prædefinerede par, uden at have den definerede "partner". <br>

3. **Fejlede overenskomster i hhv. undervisningsenheder og dagtilbud** <br>
    I undervisningsenheder må der ikke være ansættelser på overenskomsterne 46001 og 46101. I dagtilbud må der ikke være ansættelser på overenskomsterne 76001, 76101 og 77001. <br> 
    *(Flere overenskomster bliver tilføjet løbende, ligesom en foreløbig liste af accepterede overenskomster også er under udarbejdelse).*

4. **Ledere uden udløbsdato på anciennitet** <br>
    Ledere skal ansættes med en "låst" anciennitetsdato (dvs. 9999-12-31). Denne proces tjekker om ledere (defineret ved oversenskomster 45082, 45081, 46901, 45101 og 47201) har anden anciennitetsdato end den låste dato. 

5. **Forkert TRIO-placering i forhold til SD-afdeling** <br>
    Flere gange om dagen arkiveres lønfiler fra diverse skoler. Processen tjekker om de ansatte der fremgår af en skoles lønfil, er korrekt registreret. Den verificerer også at de ansatte i lønfilen eksisterer i MBU's systemer.
    Fejl registreres, hvis:
    - medarbejderen ikke har nogen aktiv XA-ansættelse
    - medarbejderen har flere aktive XA-ansættelser (datakvalitetsfejl)
    - medarbejderens SD-afdeling ikke matcher de tilladte SD-afdelinger for den pågældende TRIO-skole

## Notifikationstype
Robotten notificerer relevante modtagere om de fundne fejl. Her vælges mellem følgende muligheder

1. **Mail** (`notification_type`: `Send mail`) <br>
    Robotten sender en mail med oplysninger om fejlen. Mailen kan sendes til en fastsat person eller til en AF fællespostkasse.

eller

2. **ServiceNow sag** (`notification_type`: `Opret ServiceNow-sag`) <br>
    Under udarbejdelse. Robotten opretter en sag i ServiceNow gennem API'et. Skelettet ligger i [notifikationer/servicenow](notifikationer/servicenow/).

## Flow og kodestruktur
Robotten bliver startet gennem en trigger i OpenOrchestrator, hvor trigger properties angiver [kvalitetskontrol](#kvalitetskontroller), [notifikationstype](#notifikationsmuligheder) og modtager. Herefter identificerer robotten fejl indenfor fejltypen og konstruerer og sender notifikationer for hver fundet fejl. Hver trigger er defineret med et tidsinterval, som den bliver genaktiveret ved, <br>
Koden bliver kørt fra [main.py](main.py) og følger følgende proces:
- Den valgte kontrol slås op i `PROCESS_PROCEDURE_DICT` i [helpers/config.py](helpers/config.py) og køres fra [fejlfinding/](fejlfinding/). Kontrollen danner kø-elementer baseret på de fundne fejl.
- Hvert kø-element processeres i [processes/process_item.py](processes/process_item.py), hvor notifikationen bliver sendt med den metode, `notification_type` peger på i [notifikationer/](notifikationer/__init__.py).

### Mappestruktur
- `fejlfinding/` – én fil pr. kvalitetskontrol (`kv1.py` … `kv7.py`, `kv3_dev.py`), der finder fejl og returnerer dem som kø-elementer.
- `notifikationer/mail/` – én fil pr. kvalitetskontrol med mailens emne og tekst (`construct_email`), samt afsendelse (`sender.py`, `smtp_util.py`).
- `notifikationer/servicenow/` – oprettelse af ServiceNow-sager (under udarbejdelse).
- `helpers/ats_helpers.py` – hjælpefunktioner til Automation Server og kø-elementer.
- `helpers/proces_helpers.py` – alle hjælpefunktioner til kontrollerne og notifikationerne (databaseopslag, enheder, tillæg, løn, HTML).
- `helpers/config.py`, `helpers/process_constants.py`, `helpers/tillaeg_pairs.py` – konfiguration, konstanter og tillægspar.
- `processes/` – robotrammen: kø, fejlhåndtering, applikation og afslutning.
