import boto3
import re
import sys
import time

from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from collections import Counter

def query3(veronaCardsTable):

    """
    3. Dato un profilo, TROVARE i codici delle veronacard (id) con quel profilo che hanno fatto almeno tre strisciate 
       in uno stesso giorno, riportando il numero totale delle strisciate eseguite da quelle carte e il giorno il cui 
       sono state fatte le tre strisciate.
    """

    # TROVO TUTTE LE VERONACARDS CHE HANNO IL PROFILO RICHIESTO
    response = veronaCardsTable.scan(
        FilterExpression=Attr('Profilo').eq('24 Ore'),
        ProjectionExpression='CodiceSeriale'

    )

    veronaCards = response['Items']

    while 'LastEvaluatedKey' in response:
        response = veronaCardsTable.scan(
            ExclusiveStartKey=response['LastEvaluatedKey'],
            FilterExpression=Attr('Profilo').eq('24 Ore'),
            ProjectionExpression='CodiceSeriale'
        )
        veronaCards.extend(response['Items'])

    output = {}

    for item in veronaCards:
        veronaCard = item['CodiceSeriale']

        # TROVO GLI INGRESSI PER LA VERONACARD CORRENTE, FILTRANDO L'ITEM CHE CONTIENE LE INFO DELLA VERONACARD STESSA
        response = veronaCardsTable.query(
            KeyConditionExpression=Key('CodiceSeriale').eq(veronaCard),
            FilterExpression=Attr('Profilo').ne('24 Ore')
        )

        ingressi = response['Items']

        # CONSIDERO LE VERONACARDS CHE ABBIANO MINIMO 3 INGRESSI REGISTRATI
        if len(ingressi) < 3 :
            continue 


        # CONTO GLI INGRESSI PER OGNI DATA DISTINTA, **PER SEMPLICITÀ IMPLEMENTATIVA** PRENDO IN CONSIDERAZIONE 
        # SOLO LA PRIMA DATA CHE ABBIA MINIMO 3 INGRESSI      
        dateIngressi = [re.search(r'(\d+-\d+-\d+)',item['ChiaveOrdinamento']).group() for item in ingressi]

        ingressiPerGiorno = Counter(dateIngressi)

        for data in ingressiPerGiorno.keys():
            if ingressiPerGiorno[data] >= 3 :
                output[veronaCard] = (data,ingressiPerGiorno[data])
                break
    
    for codiceSeriale in output.keys():
        print(f'{codiceSeriale} : {output[codiceSeriale]}')

"""----------------------------------------------------------------------------------------------------------------------------------"""

def query2(veronaCardsTable,dispositiviTable):

    """
    2. Trovare il POI che ha avuto il numero minimo e il POI che ha avuto il numero massimo di accessi in un giorno assegnato.
    """

    # TROVO TUTTI GLI INGRESSI DEL GIORNO D'INTERESSE
    response = veronaCardsTable.scan(
        FilterExpression = Attr('ChiaveOrdinamento').contains('_30-12-14_')
    )
    ingressi = response['Items']

    while 'LastEvaluatedKey' in response:
        response = veronaCardsTable.scan(
            ExclusiveStartKey=response['LastEvaluatedKey'],
            FilterExpression = Attr('ChiaveOrdinamento').contains('_30-12-14_')
        )
        ingressi.extend(response['Items'])
    

    # RICOSTRUISCO UNA MAPPA AVENTE I POI COME CHIAVE E LA LISTA DI DISPOSITIVI COME VALORE
    response = dispositiviTable.scan()
    
    pointOfinterest = {}
    for item in response['Items']:
        if item['Name'] not in pointOfinterest.keys():
            pointOfinterest[item['Name']] = [item['Codice']]
        else:
            pointOfinterest[item['Name']].append(item['Codice'])

    # INIZIALIZZO UNA MAPPA CHE CONTA I GLI INGRESSI PER OGNI POI
    ingressiPerPOI = {}
    for poi in pointOfinterest.keys():
        ingressiPerPOI[poi] = 0

    # PER OGNI INGRESSO TROVATO, CONTROLLO A QUALE POI FA RIFERIMENTO E AUMENTO IL CONTATORE DEGLI INGRESSI PER QUEL POI
    for item in ingressi:
        dispositivo = re.search(r'(\d+_)',item['ChiaveOrdinamento']).group()[:-1]
        for poi in pointOfinterest.keys():
            if dispositivo in pointOfinterest[poi]:
                ingressiPerPOI[poi] = ingressiPerPOI[poi] + 1 

    # NON TUTTI I POI PRESENTI NEL SISTEMA SONO STATI ACCEDUTI NEL GIORNO D'INTERESSE, PERCIÒ LI ELIMINIAMO
    keysToDelete = []
    for poi in ingressiPerPOI.keys():
        if ingressiPerPOI[poi] == 0:
            keysToDelete.append(poi)

    for key in keysToDelete:
        del ingressiPerPOI[key]


    # TROVO IL MASSIMO ED IL MINIMO
    min = sys.maxsize
    place = ''

    for poi in ingressiPerPOI.keys():
        if ingressiPerPOI[poi] < min:
            min = ingressiPerPOI[poi]
            place = poi
    
    print('POI che ha registrato il minimo numero di ingressi nel giorno 30-12-14:')
    print((place,min))

    max = 0
    place = ''
    for poi in ingressiPerPOI.keys():
        if ingressiPerPOI[poi] > max:
            max = ingressiPerPOI[poi]
            place = poi

    print('POI che ha registrato il massimo numero di ingressi nel giorno 30-12-14:')
    print((place,max))

"""----------------------------------------------------------------------------------------------------------------------------------"""

def find_enterings_in_POI(veronaCardsTable,dispositiviTable,month,year,POI):


    # TROVO IL CODICE DEL DISPOSITIVO DEL POI
    response = dispositiviTable.query(
        IndexName = 'IndiceName',
        KeyConditionExpression = Key('Name').eq(POI)
    )

    dispositivo = response['Items'][0]['Codice']


    # TROVO GLI INGRESSI EFFETTUATI SU QUEL DISPOSITIVO, NEL MESE E ANNO INDICATI
    response = veronaCardsTable.scan(
        FilterExpression = Attr('ChiaveOrdinamento').begins_with(dispositivo)&Attr('ChiaveOrdinamento').contains('-'+month+'-'+year+'_')
    )
    ingressi = response['Items']

    while 'LastEvaluatedKey' in response:
        response = veronaCardsTable.scan(
            ExclusiveStartKey=response['LastEvaluatedKey'],
            FilterExpression = Attr('ChiaveOrdinamento').begins_with(dispositivo)&Attr('ChiaveOrdinamento').contains('-'+month+'-'+year+'_')
        )
        ingressi.extend(response['Items'])

    # COSTRUISCO UNA MAPPA AVENTE COME CHIAVE LA DATA E COME VALORE IL CONTEGGIO DEGLI INGRESSI
    output = {}

    for item in ingressi:
        date = re.search(r'(\d+-\d+-\d+)',item['ChiaveOrdinamento']).group()

        if date not in output.keys():
            output[date] = 1
        else:
            output[date] = output[date] + 1 

    output = dict(sorted(output.items()))

    return output

def query1(veronaCardsTable,dispositiviTable):

    """
    1. Assegnato un mese di un anno, trovare per ogni giorno del mese il numero totale di accessi ai due POI dati in input
    """

    out1 = find_enterings_in_POI(veronaCardsTable,dispositiviTable,'12','14','Tomba Giulietta')
    out2 = find_enterings_in_POI(veronaCardsTable,dispositiviTable,'12','14','Casa Giulietta')

    print('Daily count of enterings in Tomba Giulietta')
    for key in out1.keys():
        print((key,out1[key]))

    print('Daily count of enterings in Casa Giulietta')
    for key in out2.keys():
        print((key,out2[key]))


"""----------------------------------------------------------------------------------------------------------------------------------"""

def main():

    dynamoDbClient = boto3.resource(
        'dynamodb',
        endpoint_url = 'http://localhost:8000',
        region_name = 'eu-west-1',
        aws_access_key_id = 'user',
        aws_secret_access_key= 'password'
    )

    veronaCardsTable = dynamoDbClient.Table('VeronaCards')
    dispositiviTable = dynamoDbClient.Table('Dispositivi')

    start1 = time.time()
    query1(veronaCardsTable,dispositiviTable)
    end1 = time.time()

    print(f'query 1 took : {end1-start1} seconds')
    
    start2 = time.time()
    query2(veronaCardsTable,dispositiviTable)
    end2 = time.time()

    print(f'query 2 took : {end2-start2} seconds')
    
    start3 = time.time()
    query3(veronaCardsTable)
    end3 = time.time()

    print(f'query 3 took : {end3-start3} seconds')

if __name__ == "__main__":
    main()
