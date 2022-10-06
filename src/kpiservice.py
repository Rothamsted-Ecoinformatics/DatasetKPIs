from datetime import datetime
import json
import csv
from data.mongo import MongoRepository as mdb
from etlloaders.dataciteETL import DataCiteETL as dc
from etlloaders.zenodoETL import ZenodoETL as zen


##TODO: REFACTOR TO USE REGISTRY LOOP
#SETUP
registry = open('src/registry.json')
registrydata = json.load(registry)
mongodb = mdb()


dc = dc(registrydata["datasources"][0], mongodb)
dcresults = dc.executeKPI()
zen = zen(registrydata["datasources"][1], mongodb)
zenresults = zen.executeKPI()


#CREATE CSV
date = datetime.now().strftime("%Y_%m_%d-%I-%M-%S_%p")
print(f"filename_{date}")

file = open(f"src/archive/kpiData{date}.json" ,'w')

#WRITE JSON
jsondata = {
    "createdon": date,
    "datasources":
                    {
                    "datacite": dcresults,
                    "zenodo": zenresults
                    }
}

file.write(json.dumps(jsondata, indent=2))
file.close()



#print(str(dcresults.keys()))
#for x in dcresults["publicationCountByYear"]:
 #   print(x)



