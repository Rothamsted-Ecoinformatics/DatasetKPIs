import json
import csv
from data.mongo import MongoRepository as mdb
from sources.datacite.dataciteETL import DataCiteETL as dc
from sources.zenodo.zenodoETL import ZenodoETL as zen

registry = open('src/registry.json')
registrydata = json.load(registry)
mongodb = mdb()

dc = dc(registrydata["datasources"][0], mongodb)
dcresults = dc.executeKPI()
zen = zen(registrydata["datasources"][1], mongodb)
zenresults = zen.executeKPI()

#CREATE CSV
file = open('src/kpiData.json','w+')

#WRITE JSON
jsondata = {
    "createdon": "some date",
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



