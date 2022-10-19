
from pymongo import MongoClient, errors

#     pwd='Dollar1slandfarm',
#     roles=[{'role': 'readWrite', 'db': 'PublishedDatasets'}]

class MongoRepository:
    def __init__(self):
        self.client = MongoClient("mongodb://datasetAppUser:Dollar1slandfarm@uranus.rothamsted.ac.uk:27017/?authMechanism=SCRAM-SHA-1&authSource=PublishedDatasets")
        self.db = self.client["PublishedDatasets"]

    def getdb(self):
        return self.db

    def getcollection(self, sourcename, etlLevel):
        collectionName = etlLevel + sourcename
        return self.db[collectionName]

    def saveOne(self, tablename, data):
        print("saving to table " + tablename)


    def truncateAndInsert(self, targetCol, data, etlLevel):
        collectionName = etlLevel + targetCol
        collection = self.db[collectionName]   
        b = list(data)

        if collectionName in self.db.list_collection_names():
            collection.delete_many({}) 

        collection.insert_many(b)

    def insert(self, targetCol, data, etlLevel):
        collectionName = etlLevel + targetCol
        collection = self.db[collectionName]   
        b = list(data)
        for doc in b:
            try:
                collection.insert_one(doc)
            except errors.DuplicateKeyError:
                # skip document because it already exists in new collection
                continue
        #collection.insert_many(b)


