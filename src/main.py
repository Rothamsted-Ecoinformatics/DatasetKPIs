from services.kpiservice import KPIService
from services import apiservice
from services.testFilePath import TestRegistryPath

# if TestRegistryPath():
#     apiservice.execute()
#     kpiService = KPIService()
#     kpiService.report()

print("Registry path is " + str(TestRegistryPath()))