import time
import requests
import json
from bson.json_util import dumps


class DataCiteETL: #, IDataSource
    def __init__(self, regData, mdb):
        self.regData = regData
        self.mdb = mdb
        self.rawcol = self.mdb.getcollection(self.regData["name"], "raw") #rawcollection
        self.stagingcol = self.mdb.getcollection(self.regData["name"], "staging") #rawcollection
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
        #self.extract()
        
        #getKPIs(regData)
        self.transform()
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
        ##use api url and payload data from the registry to get raw data
        print("apiurl : " + self.regData["apiurl"])
        print("payload : " + str(self.regData["payload"]))
        response = requests.get(self.regData["apiurl"], params=self.regData["payload"])
        kpi_data = json.loads(response.text)

        ##Granularity: split the results up into individual datasets.
        datasets = []
        i = 1
        for dataset in kpi_data['data']:
            datasets.append(dataset)
            i+=1

        ##Save all the datasets to the correct collection (overwrites existing).    
        self.mdb.saveMany(self.regData["name"], datasets, "raw")

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
    

    def transform(self):
        #start_time = time.time()
        #at this point self.stagingcol.find() is empty when the class is initialised
        initialData = self.rawcol.find()
        self.mdb.saveMany(self.regData["name"], initialData, "staging")
        #at this point... the collection in the db is full, but self.stagingcol is still empty... 
        #so the filter would not work if using self.staging col. 
        #self.stagingcol = self.mdb.getcollection(self.regData["name"], "staging")
        #now we have 'reinitialised' self.staging col. 
        print("starting Filter")
        self.FilterTest()
        #self.FilterTest2()
        #self.FilterTest3()
        #read fromdatabase

        #load into staging table 

    def FilterTest(self):
        filteredResults = self.stagingcol.find({ "attributes.creators.affiliation": {"$regex" : "(?i)(Rothamsted)"} })
        self.mdb.saveMany(self.regData["name"], filteredResults, "staging")
        pass


    def load(self):
        print('load datacite data from staging to datamart')
        #copy from staging to datamart tables
        #truncate staging tables

