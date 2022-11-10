
import json

from data.mongo import MongoRepository as mdb
from services.etlloaderservice import load as GetEtlLoaders

def execute():
    registry = open('../src/registry.json')
    registrydata = json.load(registry)
    mongodb = mdb()

    #GET A LIST OF ALL THE ETLLOADER CLASSES
    # loaderClasses = GetEtlLoaders("./src/etlloaders")
    loaderClasses = GetEtlLoaders("etlloaders")
    print("loader classes: " + str(loaderClasses))

    #USE Registry NAME as key for instantiating classescvls

    for i in registrydata['datasources']:
        loaderClassName = i["name"] + "ETL"
        _class = loaderClasses[loaderClassName]
        loader = _class(i, mongodb)
        # loader.executeETL()
        loader.extract()
        loader.transform()

    registry.close()

    # kpiService = KPIService()
    # kpiService.publicationCountByYear()
    # kpiService.report()

# dc = dc(registrydata["datasources"][0], mongodb)
# dc.executeETL()
# zen = zen(registrydata["datasources"][1], mongodb)
# zen.executeETL()


