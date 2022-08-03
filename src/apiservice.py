
import json
from data.mongo import MongoRepository as mdb
from sources.datacite.dataciteETL import DataCiteETL as dc
from sources.zenodo.zenodoETL import ZenodoETL as zen

registry = open('src/registry.json')
registrydata = json.load(registry)
mongodb = mdb()

dc = dc(registrydata["datasources"][0], mongodb)
dc.executeETL()
zen = zen(registrydata["datasources"][1], mongodb)
zen.executeETL()


