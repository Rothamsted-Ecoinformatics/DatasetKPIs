
from pymongo import MongoClient

#     pwd='Dollar1slandfarm',
#     roles=[{'role': 'readWrite', 'db': 'PublishedDatasets'}]

class MongoRepository:
    def __init__(self):
        self.client = MongoClient("mongodb://datasetAppUser:Dollar1slandfarm@uranus.rothamsted.ac.uk:27017/?authMechanism=SCRAM-SHA-1&authSource=PublishedDatasets")
        self.db = self.client["PublishedDatasets"]

    def getdb(self):
        return self.db

    def getcollection(self, sourcename):
        collectionName = "raw" + sourcename
        return self.db[collectionName]

    def saveOne(self, tablename, data):
        print("saving to table " + tablename)


    def saveMany(self, sourcename, datasets):
        print("MongoDB saving many")
        collectionName = "raw" + sourcename

        if collectionName in self.db.list_collection_names():
            print("Collection " + collectionName + " already exists")
        else:
            print("Collection " + collectionName + " does not exist")

        collection = self.db[collectionName]   
        collection.delete_many({}) 
        collection.insert_many(datasets)


