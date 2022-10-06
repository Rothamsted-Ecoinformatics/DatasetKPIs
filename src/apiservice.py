
import json
from data.mongo import MongoRepository as mdb
from services.etlloaderservice import load as GetEtlLoaders

#initialise registry and db
registry = open('src/registry.json')
registrydata = json.load(registry)
mongodb = mdb()

#GET A LIST OF ALL THE ETLLOADER CLASSES
loaderClasses = GetEtlLoaders("./src/etlloaders")
print("loader classes: " + str(loaderClasses))

#USE Registry NAME as key for instantiating classes
for i in registrydata['datasources']:
    loaderClassName = i["name"] + "ETL"
    _class = loaderClasses[loaderClassName]
    loader = _class(i, mongodb)
    loader.executeETL()

registry.close()



# dc = dc(registrydata["datasources"][0], mongodb)
# dc.executeETL()
# zen = zen(registrydata["datasources"][1], mongodb)
# zen.executeETL()


