

from pymongo import MongoClient

client = MongoClient("mongodb://pdAdmin:StreamsF4rmSim!lar@uranus.rothamsted.ac.uk:27017/?authMechanism=SCRAM-SHA-1&authSource=PublishedDatasets")
#print(client.list_database_names())

db = client["PublishedDatasets"]
listing = db.command('usersInfo')
for x in listing['users']:
    print(x)

#creating datasetAPP user
#db.command(
#    'createUser', 'datasetAppUser', 
#    pwd='Dollar1slandfarm',
#    roles=[{'role': 'readWrite', 'db': 'PublishedDatasets'}]
#)

#creating datasetAPP user
#db.command(
#    'createUser', 'pdReadOnlyUser', 
#    pwd='TriedJup1terplains',
#    roles=[{'role': 'read', 'db': 'PublishedDatasets'}]
#)


    ## {
       #     "name": "Zenodo",
       #     "apiurl": "https://zenodo.org/api/records",
       #     "payload": {
       #         "q": "creators.affiliation:Rothamsted AND resource_type.type:(dataset OR software)",
       #         "size": 1000
       #     }
       # }



