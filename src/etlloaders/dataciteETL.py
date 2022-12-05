import json
import time
import requests
import transformations.filters as filter

class DataCiteETL:  # , IDataSource
    def __init__(self, regData, mdb):
        self.regData = regData
        self.mdb = mdb
        self.rawcol = self.mdb.getcollection(self.mdb.getrawdb(), self.regData["name"])
        self.stagingcol = self.mdb.getcollection(self.mdb.getstagingdb(), self.regData["name"])
        self.archivecol = self.mdb.getcollection(self.mdb.getarchivedb(), self.regData["name"])
        self.reportingCol = self.mdb.getcollection(self.mdb.getreportingdb(),
                                                   self.regData["name"])  # datawarehouse persistence collection
        self.clients = {
            "cern.zenodo": "Zenodo",
            "bl.rothres": "Rothamsted Research",
            "bl.nerc": "NERC Environmental Data Service",
            "gbif.gbif": "Global Biodiversity Information Facility",
            "figshare.ars": "figshare Academic Research System",
            "gdcc.harvard-dv": "Harvard Dataverse",
            "bl.f1000r": "Faculty of 1000 Research Ltd",
            "dryad.dryad": "DRYAD",
            "gdcc.icraf": "International Centre for Research in Agroforestry (ICRAF)",
            "bl.mendeley": "Mendeley Data",
            "rg.rg": "ResearchGate",
            "bl.stfc": "Science and Technology Facilities Council",
            "bl.cabi": "Centre for Agriculture and Biosciences International",
            "bl.bristol": "University of Bristol",
            "cdl.ucsb": "KNB Data Repository",
            "bl.reading": "University of Reading",
        }

    def executeETL(self):
        print('executing full etl pipeline for ' + self.regData["name"])
        # print(str(self.rawcol.name))
        # self.extract()
        # self.transform()
        # load()
        # getKPIs(regData)

    ###query apis and dump data into raw - arching old data.
    def extract(self):
        print("Calling " + self.regData["apiurl"] + ": " + self.regData["name"])
        response = requests.get(self.regData["apiurl"], json=self.regData["payload"])
        # TODO: Catch 500 error codes from server.
        print(response.status_code)
        kpi_data = json.loads(response.text)
        datasets = []
        i = 1
        for dataset in kpi_data['data']:
            datasets.append(dataset)
            i += 1
        self.mdb.truncateAndInsert(self.rawcol, datasets)

    def transform(self):
        # self.stagingcol.delete_many({})
        self.mdb.archiveAndTruncate(self.stagingcol)
        filter.byFieldWithRegEx(self, "attributes.creators.affiliation", "(?i)(Rothamsted)")
        filter.byFieldWithRegEx(self, "attributes.creators.name", "(?i)(Rothamsted)")

    def load(self):
        print('load datacite data from staging to datamart')

        # copy from staging to datamart tables
        # truncate staging tables

    # def executeKPI(self):
    #     # self.getKPIs()
    #     y = self.getPublicationCountByYear()
    #     p = self.getPublicationCountByPublisher()
    #     yc = self.getPublicationCountByYearByClient()
    #     result = {
    #         "publicationCountByYear": list(y),
    #         "publicationCountByPublisher": list(p),
    #         "publicationCountByYearByClient": list(yc)
    #     }
    #     return result

    def getPublicationCountByYear(self):
        publicationCountByYear = self.stagingcol.aggregate([
            {
                "$group": {
                    "_id": {
                        "$year": {"$dateFromString":
                            {
                                "dateString": "$attributes.created"
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

    def getPublicationCountByPublisher(self):
        publicationCountByPublisher = self.stagingcol.aggregate([
            {
                "$group": {
                    "_id": "$relationships.client.data.id",
                    "num_datasets": {"$sum": 1}
                }
            },
            {"$sort": {"num_datasets": -1}}
        ])

        resultList = list(publicationCountByPublisher)

        for result in resultList:
            clientName = ''
            if result['_id'] in self.clients:
                clientName = self.clients[result['_id']]

            result['client_name'] = clientName

        return resultList

    def getPublicationCountByYearByClient(self):
        start_time = time.time()
        publicationCountByYearByClient = self.stagingcol.aggregate([
            {
                "$group": {
                    "_id": {
                        "year": {"$year": {"$dateFromString":
                            {
                                "dateString": "$attributes.created"
                            }
                        }},
                        "client_id": "$relationships.client.data.id"
                    },
                    "num_datasets": {"$sum": 1}
                }
            },
            {"$sort": {"_id.year": 1, "num_datasets": -1}}
        ])

        resultList = list(publicationCountByYearByClient)

        for result in resultList:
            clientName = ''
            if result['_id']['client_id'] in self.clients:
                clientName = self.clients[result['_id']['client_id']]

            result['_id']['client_name'] = clientName

        return resultList
