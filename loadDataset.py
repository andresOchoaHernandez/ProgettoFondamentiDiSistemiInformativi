import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

import csv
import glob

LIMIT = 1000

def create_tables(dynamoDbClient):
    try:
        dynamoDbClient.create_table (
            TableName = 'VeronaCards',
            KeySchema = [
                {
                    'AttributeName': 'CodiceSeriale',
                    'KeyType'      : 'HASH'
                },
                {
                    'AttributeName': 'ChiaveOrdinamento',
                    'KeyType'      : 'RANGE'
                }
            ],
            AttributeDefinitions = [
                {
                    'AttributeName': 'CodiceSeriale',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'ChiaveOrdinamento',
                    'AttributeType': 'S'
                }
            ],
            GlobalSecondaryIndexes = [{
                'IndexName' : 'IndiceChiaveOrdinamento',
                'KeySchema' : [
                    {
                        'AttributeName': 'ChiaveOrdinamento',
                        'KeyType': 'HASH'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            }],
            ProvisionedThroughput={
                'ReadCapacityUnits':10,
                'WriteCapacityUnits':10
            }   
        )
    except:
        print('La tabella VeronaCards è già presente nel sistema')

    try:
        dynamoDbClient.create_table (
            TableName = 'Dispositivi',
            KeySchema = [
                {
                    'AttributeName': 'Codice',
                    'KeyType'      : 'HASH'
                },
                {
                    'AttributeName': 'Name',
                    'KeyType'      : 'RANGE'
                }
            ],
            AttributeDefinitions = [
                {
                    'AttributeName': 'Codice',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'Name',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits':10,
                'WriteCapacityUnits':10
            }   
        )
    except:
        print('La tabella Dispositivi è già presente nel sistema')

def populate_tables(dynamoDbClient):
    veronaCardsTable = dynamoDbClient.Table('VeronaCards')
    dispositiviTable = dynamoDbClient.Table('Dispositivi')

    listOfCards = {}
    listOfIngressi = []
    listOfDispositivi = {}

    limit = 100

    for filename in glob.glob('./dataset/*.csv'):
        
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file,delimiter=',')

            for row in csv_reader:
                veronaCard = {}
                ingresso = {}
                dispositivo = {}

                """
                Inserimento VeronaCard
                """
                if row[4] not in listOfCards.keys() and len(listOfCards) <= 24: 
                    veronaCard['CodiceSeriale'] = row[4]
                    veronaCard['ChiaveOrdinamento'] = 'Info'
                    veronaCard['DataAttivazione'] = row[5]
                    veronaCard['Profilo'] = row[8]

                    listOfCards[row[4]] = veronaCard
                
                if len(listOfCards) == 25:
                    with veronaCardsTable.batch_writer() as batch:
                        for item in listOfCards.keys():
                            batch.put_item(Item=listOfCards[item])
                    listOfCards.clear()
                
                """
                Inserimento Ingresso
                """
                if len(listOfIngressi) <= 24 : 
                    ingresso['CodiceSeriale'] = row[4]
                    ingresso['ChiaveOrdinamento'] = row[3] + "_" + row[0] +"_" + row[1]

                    listOfIngressi.append(ingresso)

                if len(listOfIngressi) == 25:
                    with veronaCardsTable.batch_writer() as batch:
                        for item in listOfIngressi:
                            veronaCardsTable.put_item(Item = item)
                    listOfIngressi.clear()

                """
                Inserimento Dispositivo
                """
                if row[3] not in listOfDispositivi.keys() and len(listOfDispositivi) <= 4:
                    dispositivo['Codice'] = row[3]
                    dispositivo['Name'] = row[2]

                    listOfDispositivi[row[3]] = dispositivo
                
                if len(listOfDispositivi) == 5:
                    with dispositiviTable.batch_writer() as batch:
                        for item in listOfDispositivi.keys():
                            batch.put_item(Item=listOfDispositivi[item])
                    listOfDispositivi.clear()

                """
                Limite per il testing
                """
                if limit == LIMIT:
                    limit = 0
                    break

                limit+=1

def scan_table(dynamoDbClient,table_name):
    response = dynamoDbClient.Table(table_name).scan()
    for item in response['Items']:
        print(item)

def main():

    dynamoDbClient = boto3.resource(
        'dynamodb',
        endpoint_url = 'http://localhost:8000',
        region_name = 'eu-west-1',
        aws_access_key_id = 'user',
        aws_secret_access_key= 'password'
    )

    create_tables(dynamoDbClient)
    populate_tables(dynamoDbClient)
    scan_table(dynamoDbClient,'VeronaCards')

    """
    Ottenere una certa veronacard
    """
    response = dynamoDbClient.Table('VeronaCards').get_item(TableName='VeronaCards',Key={'CodiceSeriale':'04FA80523F3880','ChiaveOrdinamento':'Info'})
    print(response['Item'])

    print("================================================================")

    """
    Ingressi di una certa veronacard con un certo dispositivo
    """
    response = dynamoDbClient.Table('VeronaCards').query(TableName='VeronaCards',KeyConditionExpression= Key('CodiceSeriale').eq('04FA80523F3880')&Key('ChiaveOrdinamento').begins_with('40'))
    for item in response['Items']:
        print(item)

    print("================================================================")
    
    """
    Ingressi in un certo dispositivo
    """
    response = dynamoDbClient.Table('VeronaCards').scan(FilterExpression=Attr('ChiaveOrdinamento').begins_with('40'))
    for item in response['Items']:
        print(item)

    print("================================================================")

    """
    Ingressi in una certa data
    """
    response = dynamoDbClient.Table('VeronaCards').scan(FilterExpression=Attr('ChiaveOrdinamento').contains('30-12-16'))
    for item in response['Items']:
        print(item)

    """
    Scan sulla sort key sfruttando l'indice secondario globale
    """
    response = dynamoDbClient.Table('VeronaCards').scan(TableName='VeronaCards',IndexName='IndiceChiaveOrdinamento',FilterExpression=Attr('ChiaveOrdinamento').begins_with('40'))
    for item in response['Items']:
        print(item)

if __name__ == '__main__':
    main()