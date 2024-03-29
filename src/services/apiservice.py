
import json
from data.mongo import MongoRepository as mdb
from services.etlloaderservice import load as GetEtlLoaders
import config

def execute():
    # registry = open('../src/registry.json')
    registry = open(config.registryPath)
    registrydata = json.load(registry)
    mongodb = mdb()

    #GET A LIST OF ALL THE ETLLOADER CLASSES
    # loaderClasses = GetEtlLoaders("./src/etlloaders")
    loaderClasses = GetEtlLoaders("src/etlloaders")
    print("loader classes: " + str(loaderClasses))

    #USE Registry NAME as key for instantiating classescvls

    for i in registrydata['datasources']:
        loaderClassName = i["name"] + "ETL"
        _class = loaderClasses[loaderClassName]
        loader = _class(i, mongodb)
        # loader.executeETL()
        print("Extracting from " + i["name"] + "'s api into RAW")
        loader.extract()
        print("Transforming from " + i["name"] + "'s RAW into Staging, with Archiving")
        loader.transform()

    registry.close()

    # kpiService = KPIService()
    # kpiService.publicationCountByYear()
    # kpiService.report()

# dc = dc(registrydata["datasources"][0], mongodb)
# dc.executeETL()
# zen = zen(registrydata["datasources"][1], mongodb)
# zen.executeETL()


