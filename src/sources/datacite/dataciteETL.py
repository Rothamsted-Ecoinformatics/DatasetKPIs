import time
import requests
import json
from bson.json_util import dumps


class DataCiteETL: #, IDataSource
    def __init__(self, regData, mdb):
        self.regData = regData
        self.mdb = mdb
        self.col = self.mdb.getcollection(self.regData["name"])
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
        print('executing full etl pipeline')
        self.extract()
        #getKPIs(regData)
        #transform()
        #load()

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

    def extract(self):
        start_time = time.time()
        response = requests.get(self.regData["apiurl"], params=self.regData["payload"])
        kpi_data = json.loads(response.text)

        datasets = []
        i = 1
        for dataset in kpi_data['data']:
            
            ##print("Dataset " + str(i) + ". type = " + str(type(dataset)))
            datasets.append(dataset)
            i+=1
    
        print("saving many from " + self.regData["name"])
        self.mdb.saveMany(self.regData["name"], datasets)

    def getPublicationCountByYear(self):
        publicationCountByYear = self.col.aggregate([
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
        publicationCountByPublisher = self.col.aggregate([
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
        publicationCountByYearByClient = self.col.aggregate([
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


    def transform(self):
        #start_time = time.time()
        results = self.col.find()
        #read fromdatabase
        #transform
        #load into staging table 

    def load(self):
        print('load datacite data from staging to datamart')
        #copy from staging to datamart tables
        #truncate staging tables

