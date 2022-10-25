import json
from datetime import datetime
from data.mongo import MongoRepository as mdb
from services.etlloaderservice import load as GetEtlLoaders


class KPIService:

    # CONSTRUCTOR
    def __init__(self):
        # self.registry = open('src/registry.json')
        self.registry = open('registry.json')
        self.registrydata = json.load(self.registry)
        self.mongodb = mdb()
        self.reportingdb = self.mongodb.getreportingdb()
        self.loaderClasses = GetEtlLoaders("etlloaders")
        self.registry.close()

    def report(self):
        self.getDatasourceKPI("PublicationCountByYear")
        self.getDatasourceKPI("PublicationCountByPublisher")
        self.getDatasourceKPI("PublicationCountByYearByClient")

    def getDatasourceKPI(self, KPIName):
        # datawarehouse persistence collection
        targetCol = self.mongodb.getcollection(self.reportingdb, KPIName)

        # GET A LIST OF ALL THE ETLLOADER CLASSES
        datasources = []
        func_name = "get" + KPIName

        for i in self.registrydata['datasources']:
            loaderClassName = i["name"] + "ETL"
            _class = self.loaderClasses[loaderClassName]
            loader = _class(i, self.mongodb)
            func = getattr(loader, func_name)
            try:
                datasource = {"name": i["name"], "data": func()}
                datasources.append(datasource)

            except:
                print("Loader " + loaderClassName + " does not include get " + KPIName)

        data = {
            "createdon": datetime.now().strftime("%Y_%m_%d-%I-%M-%S_%p"),
            "datasources": datasources
        }

        targetCol.insert_one(data)
