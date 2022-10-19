import time
import requests
import json
from bson.json_util import dumps
import transformations.filters as filter

class DataCiteETL: #, IDataSource
    def __init__(self, regData, mdb):
        self.regData = regData
        self.mdb = mdb
        self.rawcol = self.mdb.getcollection(self.mdb.getrawdb(), self.regData["name"]) 
        self.stagingcol = self.mdb.getcollection(self.mdb.getstagingdb(), self.regData["name"]) 
        self.archivecol = self.mdb.getcollection(self.mdb.getarchivedb(), self.regData["name"])
        self.reportingCol = self.mdb.getcollection(self.mdb.getreportingdb(), self.regData["name"]) #datawarehouse persistence collection
        self.clients = {
                "cern.zenodo": "Zenodo",
                "bl.rothres": "Rothamsted Research",
                "bl.nerc": "NERC Environmental Data Service",
                "gbif.gbif": "Global Biodiversity Information Facility",
                "figshare.ars": "figshare Academic Research System",
                "gdcc.harvard-dv":"Harvard Dataverse",
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
        #print(str(self.rawcol.name))
        self.extract()
        #self.transform()
        #load()
        #getKPIs(regData)

    ###query apis and dum[p data into raw - arching old data.
    def extract(self):
        response = requests.get(self.regData["apiurl"], params=self.regData["payload"])
        #TODO: Catch 500 error codes from server.

        kpi_data = json.loads(response.text)
        datasets = []
        i = 1
        for dataset in kpi_data['data']:
            datasets.append(dataset)
            i+=1   
        self.mdb.archiveTruncateAndInsert(self.rawcol, datasets)

    def transform(self):
        self.stagingcol.delete_many({})
        filter.byFieldWithRegEx(self,  "attributes.creators.affiliation", "(?i)(Rothamsted)")
        filter.byFieldWithRegEx(self, "attributes.creators.name", "(?i)(Rothamsted)")

    def load(self):
        
        print('load datacite data from staging to datamart')

        #copy from staging to datamart tables
        #truncate staging tables

    def executeKPI(self):
        #self.getKPIs()
        y = self.getPublicationCountByYear()
        p = self.getPublicationCountByPublisher()
        yc = self.getPublicationCountByYearByClient()
        result = {
                "publicationCountByYear": list(y),
                "publicationCountByPublisher": list(p),
                "publicationCountByYearByClient": list(yc)
                }
        return result

    def getPublicationCountByYear(self):
        publicationCountByYear = self.rawcol.aggregate([
                                                    {
                                                        "$group" : {
                                                        "_id" : { 
                                                            "$year": {"$dateFromString": 
                                                                {
                                                                "dateString": "$attributes.created"
                                                                } 
                                                            }
                                                            }, 
                                                            "num_datasets" : {"$sum" :1}
                                                            }
                                                        },
                                                    {"$sort" : {"_id" :1}}
                                            ])
        #for x in publicationCountByYear:
         #   print(x)
        return publicationCountByYear

    def getPublicationCountByPublisher(self):
        publicationCountByPublisher = self.rawcol.aggregate([
                                                    {
                                                        "$group" : {
                                                        "_id" : "$relationships.client.data.id", 
                                                            "num_datasets" : {"$sum" :1}
                                                            }
                                                        },
                                                    {"$sort" : {"num_datasets" :-1}}
                                            ])
        return publicationCountByPublisher

    def getPublicationCountByYearByClient(self):
        start_time = time.time()
        publicationCountByYearByClient = self.rawcol.aggregate([
                                                    {
                                                        "$group" : {
                                                        "_id" : { 
                                                            "year": {"$year": {"$dateFromString": 
                                                                {
                                                                "dateString": "$attributes.created"
                                                                } 
                                                            }},
                                                            "client": "$relationships.client.data.id"
                                                            }, 
                                                            "num_datasets" : {"$sum" :1}
                                                            }
                                                        },
                                                    {"$sort" : {"_id.year" :1, "num_datasets": -1}}
                                            ])
        return publicationCountByYearByClient
    



