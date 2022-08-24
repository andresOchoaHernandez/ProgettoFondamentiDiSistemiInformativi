import boto3
from boto3.dynamodb.conditions import Key

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
    listOfDispositivi = {}

    limit = 0

    for filename in glob.glob('./dataset/*.csv'):
        
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file,delimiter=',')

            for row in csv_reader:
                veronaCard = {}
                ingresso = {}
                dispositivo = {}

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
                

                ingresso['CodiceSeriale'] = row[4]
                ingresso['ChiaveOrdinamento'] = row[3]
                ingresso['Data'] = row[0]
                ingresso['Ora'] = row[1]

                veronaCardsTable.put_item(Item = ingresso)

                if row[3] not in listOfDispositivi.keys() and len(listOfDispositivi) <= 24:
                    dispositivo['Codice'] = row[3]
                    dispositivo['Name'] = row[2]

                    listOfDispositivi[row[3]] = dispositivo
                
                if len(listOfDispositivi) == 25:
                    with dispositiviTable.batch_writer() as batch:
                        for item in listOfDispositivi:
                            batch.put_item(Item=item)

                if limit == LIMIT:
                    limit = 0
                    break

                limit+=1


def main():

    dynamoDbClient = boto3.resource(
        'dynamodb',
        endpoint_url = 'http://localhost:8000',
        region_name = 'eu-west-1',
        aws_access_key_id = 'user',
        aws_secret_access_key= 'password'
    )

    #create_tables(dynamoDbClient)
    #populate_tables(dynamoDbClient)

    response = dynamoDbClient.Table('VeronaCards').get_item(TableName='VeronaCards',Key={'CodiceSeriale':'049B31523F3880','ChiaveOrdinamento':'Info'})

    print(response['Item'])

    response = dynamoDbClient.Table('VeronaCards').query(TableName='VeronaCards',KeyConditionExpression = Key('CodiceSeriale').eq('049B31523F3880'))

    for item in response['Items']:
        print(item)


if __name__ == '__main__':
    main()