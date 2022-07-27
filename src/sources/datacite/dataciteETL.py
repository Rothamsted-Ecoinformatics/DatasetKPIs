from pymongo import MongoClient
import time
import requests
import json

url = 'https://api.datacite.org/dois'
payload = {'query': 'Rothamsted AND types.resourceTypeGeneral: (Dataset OR Model OR Other OR Software)','page[size]': '1000'}

transformSourceQuery = "ddd"
loadQuery = "ddd"

def execute():
    print('executing full etl pipeline')
    extract()
    transform()
    load()


def extract():

    start_time = time.time()
    print('extract datacite data from API')

    #connect to mongo
    client = MongoClient("mongodb://superuser:AgainstMake1nsect@149.155.16.39/")
    db = client["TestDB"]
    collection = db['DataCiteRAW']
    collection.delete_many({}) 
    #call api
    response = requests.get(url, params=payload)
    #print(response.json())
    kpi_data = json.loads(response.text)

    i = 1
    for data in kpi_data['data']:
        
        print("Dataset " + str(i) + ". type = " + str(type(data)))
        collection.insert_one(data)
        i+=1
    #cursors
    #parse response
    #place into mongo #abstract database connection to repository layer


def transform():
    print('grab datacite from MONGO and transform')
    client = MongoClient("mongodb://superuser:AgainstMake1nsect@149.155.16.39/")
    db = client["TestDB"]
    collection = db['DataCiteRAW']
    #start_time = time.time()
    results = collection.find()
    #read fromdatabase
    #transform
    #load into staging table 

def load():
    print('load datacite data from staging to datamart')
    #copy from staging to datamart tables
    #truncate staging tables