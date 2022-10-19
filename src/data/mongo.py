
from os import truncate
from pymongo import MongoClient, errors
from datetime import datetime

#     pwd='Dollar1slandfarm',
#     roles=[{'role': 'readWrite', 'db': 'PublishedDatasets'}

class MongoRepository:
    def __init__(self):
        self.client = MongoClient("mongodb://datasetAppUser:Dollar1slandfarm@uranus.rothamsted.ac.uk:27017/?authMechanism=SCRAM-SHA-1&authSource=PublishedDatasets")
        self.defaultdb = self.client["PublishedDatasets"]
        self.rawdb = self.client["rawData"]
        self.stagingdb = self.client["stagingData"]
        self.archivedb = self.client["archiveData"]
        self.reportingdb = self.client["reportingData"]

    def getdefaultdb(self):
        return self.defaultdb

    def getrawdb(self):
        return self.rawdb

    def getstagingdb(self):
        return self.stagingdb

    def getarchivedb(self):
        return self.archivedb
    
    def getreportingdb(self):
        return self.reportingdb


    def getcollection(self, db, sourcename):
        collectionName = sourcename
        return db[collectionName]

    def saveOne(self, tablename, data):
        print("saving to table " + tablename)

    def archiveTruncateAndInsert(self, targetCol, data):
        #archive the existing data from the target collection
        try:
            self.archivedb[str(targetCol.name) + str(datetime.now())].insert_many(targetCol.find())
        except errors.InvalidOperation:
            pass
        #trucnate the target collection
        self.truncateAndInsert(targetCol, data)

    def truncateAndInsert(self, targetCol, data):
        b = list(data)
        try:
            targetCol.delete_many({}) 
        except errors.CollectionInvalid:
            #comnent
            pass
        targetCol.insert_many(b)

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


