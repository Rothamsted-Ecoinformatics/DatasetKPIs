
apiquery = "ddd"
transformSourceQuery = "ddd"
loadQuery = "ddd"

def execute():
    print('executing full etl pipeline')
    extract()
    transform()
    load()



def extract():
    print('extract datacite data from API')
    #call api
    #cursors
    #parse response
    #place into mongo #abstract database connection to repository layer


def transform():
    print('grab datacite from MONGO and transform')
    #read fromdatabase
    #transform
    #load into staging table 

def load():
    print('load datacite data from staging to datamart')
    #copy from staging to datamart tables
    #truncate staging tables