import time
import requests
import json


class ZenodoETL: #, IDatasource
    def __init__(self, regData, mdb):
        self.regData = regData
        self.mdb = mdb
        self.col = self.mdb.getcollection(self.regData["name"])

    def executeETL(self):
        self.extract()

    def executeKPI(self):
        dt = self.getDownloadsByDataType()
        t = self.getDownloadsByTitle()
        result = {
            "downloadsByDataType": list(dt),
            "downloadsByTitle": t
        }
        return result

    def extract(self):
        start_time = time.time()
        response = requests.get(self.regData["apiurl"], params=self.regData["payload"])

        kpi_data = json.loads(response.text)

        datasets = []
        i = 1
        for dataset in kpi_data['hits']['hits']:
            ##print("Ddataset: " + str(i) + " doi is " + dataset['doi'])
            datasets.append(dataset)
            #print("Dataset " + str(i) + ". type = " + str(type(data)))
            #collection.insert_one(data)
            i+=1
        
        print("saving many from " + self.regData["name"])
        self.mdb.saveMany(self.regData["name"], datasets)


    def getDownloadsByDataType(self):
        downloadsByDataType = self.col.aggregate([
            {
                "$group": {
                    "_id": "$metadata.resource_type.type",
                        "count": {"$sum": 1},
                        "downloads": {"$sum": "$stats.downloads"} 
                }
            }    
        ])
        return downloadsByDataType

    def getDownloadsByTitle(self):
        downloadsByTitle = self.col.find({},{ "_id": 0, "metadata.title": 1, "stats.downloads": 1 })
        print("ZENODO: DOWNLOADS BY TITLE")
        mydict = {
            "1" :{
                    "title": "",
                    "download": 98,
                    "views": 98
                },
            "2" :{
                "title": "",
                "download": 98,
                "views": 98
            }

        }
        i = 0
        for x in downloadsByTitle:
            mydict[str(i)] = {
                                "title": x['metadata']['title'],
                                "downloads": x['stats']['downloads']
                                }

            i+=1
            
        return mydict

