import datetime

from data.mongo import MongoRepository as mdb
import json


def compare():
    db = mdb()
    rawids = set(db.getrawdb()['DataCite'].find().distinct('_id'))
    stageids = set(db.getstagingdb()['DataCite'].find().distinct('_id'))
    diffids = rawids - stageids

    # diffs = db.getrawdb()['DataCite'].find({"_id": {"$in": list(diffids)}})
    diffs = db.getrawdb()['DataCite'].find({"$and": [
        {"attributes.publisher": {"$ne": "The Global Biodiversity Information Facility"}},
        {"attributes.descriptions.description": {"$ne": {"$regex": "A dataset containing"}}},
        {"_id": {"$in": list(diffids)}}]})
    dictionary = {'createdOn': str(datetime.datetime.now()), 'excludedDocs': list(diffs)}
    # for doc in diffs:
    #     type(doc)
    #     dictionary

    # print(list(diffs))

    # Serializing json
    with open("sample.json", "w+") as outfile:
        json.dump(dictionary, outfile, default=str, indent=4)


compare()
