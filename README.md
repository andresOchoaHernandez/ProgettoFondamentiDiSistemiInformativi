# ProgettoFondamentiDiSistemiInformativi
Progetto per il corso di fondamenti di sistemi informativi ad UniVr

## Obbiettivo
Il progetto prevede di scegliere un dataset ed un sistema(tra quelli visti durante il corso) e produrre:
* Uno schema concettuale del dataset (UML)
* Uno schema fisico del dataset per il sistema scelto (UML)  

Successivamente caricare il dataset sul sistema ed eseguire query accordate col professore, per poi valutarne le performance.

Ho scelto il sistema Amazon DynamoDb, ed il dataset VeronaCard.

# Dataset
Il dataset VeronaCard consiste in 7 file csv contenti collezioni di strisciate delle VeronaCards su dei dispositivi, dall'anno 2014 al 2020.
Tutti i file rispettano la stessa struttura, in cui ogni record contiene:
- Data ingresso
- Ora ingresso
- Point of interest
- Dispositivo su cui è stata effettuata la strisciata della VeronaCard
- Codice seriale della VeronaCard
- Data di attivazione della VeronaCard
- Profilo della VeronaCard (24-48-72 Ore)

## Schema concettuale del dataset
