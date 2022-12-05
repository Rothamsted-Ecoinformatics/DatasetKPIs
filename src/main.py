import services.testFilePath as test
import services.apiservice as apiservice
from services.kpiservice import KPIService

if test.TestRegistryPath():
    print("Running API service")
    apiservice.execute()
    kpiService = KPIService()
    print("Running KPI service reporting")
    kpiService.report()

print("Registry path is " + str(test.TestRegistryPath()))
print("Registry path is " + str(test.TestRegistryPathConfig()))
