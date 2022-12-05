
from pymongo import MongoClient
import time
import requests
import json

url = ""
payload = {}

def execute():
    extract()

def extract():
    start_time = time.time()
    print('extract datacite data from API')

    #connect to mongo
    client = MongoClient("mongodb://superuser:AgainstMake1nsect@149.155.16.39/")
    db = client["TestDB"]
    collection = db['FigshareRAW']
    collection.delete_many({}) 
    #call api
    response = requests.get(url, json=payload)
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