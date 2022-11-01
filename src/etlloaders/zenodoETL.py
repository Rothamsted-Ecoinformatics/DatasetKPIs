import time
import requests
import json
import src.transformations.filters as filter


class ZenodoETL:  # , IDatasource
    def __init__(self, regData, mdb):
        self.regData = regData
        self.mdb = mdb
        # self.col = self.mdb.getcollection(self.regData["name"])
        self.rawcol = self.mdb.getcollection(self.mdb.getrawdb(), self.regData["name"])
        self.stagingcol = self.mdb.getcollection(self.mdb.getstagingdb(), self.regData["name"])
        self.archivecol = self.mdb.getcollection(self.mdb.getarchivedb(), self.regData["name"])
        self.reportingCol = self.mdb.getcollection(self.mdb.getreportingdb(),
                                                   self.regData["name"])  # datawarehouse persistence collection

    def executeETL(self):
        # self.extract()
        pass

    def executeKPI(self):
        dt = self.getDownloadsByDataType()
        t = self.getDownloadsByTitle()
        result = {
            "downloadsByDataType": list(dt),
            "downloadsByTitle": t
        }
        return result

    def extract(self):
        # start_time = time.time()
        print("Calling " + self.regData["apiurl"] + ": " + self.regData["name"])
        response = requests.get(self.regData["apiurl"], params=self.regData["payload"])
        print(response.status_code)

        kpi_data = json.loads(response.text)

        datasets = []
        i = 1
        for dataset in kpi_data['hits']['hits']:
            datasets.append(dataset)
            i += 1
        self.mdb.truncateAndInsert(self.rawcol, datasets)

    def transform(self):
        self.mdb.archiveAndTruncate(self.stagingcol)
        filter.byFieldWithRegEx(self, "metadata.creators.affiliation", "(?i)(Rothamsted)")
        filter.byFieldWithRegEx(self, "metadata.creators.name", "(?i)(Rothamsted)")

    def getDownloadsByDataType(self):
        downloadsByDataType = self.stagingcol.aggregate([
            {
                "$group": {
                    "_id": "$metadata.resource_type.type",
                    "count": {"$sum": 1},
                    "downloads": {"$sum": "$stats.downloads"}
                }
            }
        ])
        return list(downloadsByDataType)

    def getDownloadsByTitle(self):
        downloadsByTitle = self.stagingcol.find({}, {"_id": 0, "metadata.title": 1, "stats.downloads": 1})
        # print("ZENODO: DOWNLOADS BY TITLE")
        mydict = {}
        resultList = []
        i = 0
        for x in downloadsByTitle:
            mydict = {
                "title": x['metadata']['title'],
                "downloads": x['stats']['downloads']
            }
            resultList.append(mydict)

            i += 1
        return resultList

    def getPublicationCountByYear(self):
        publicationCountByYear = self.stagingcol.aggregate([
            {
                "$group": {
                    "_id": {
                        "$year": {"$dateFromString":
                            {
                                "dateString": "$created"
                            }
                        }
                    },
                    "num_datasets": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ])
        # for x in publicationCountByYear:
        #   print(x)
        return list(publicationCountByYear)
