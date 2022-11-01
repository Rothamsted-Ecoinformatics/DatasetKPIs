from pymongo import MongoClient

#client is the whole MongoServer
client = MongoClient("mongodb://pdReadOnly:TriedJup1terplains@uranus.rothamsted.ac.uk:27017/?authMechanism=SCRAM-SHA-1&authSource=PublishedDatasets")
#db is the individual database
db = client["PublishedDatasets"]

x = db['rawdatacite'].count_documents({})

for y in x: 
    print(y)

#list all collections in the database
#print(db.list_collections())
