def byFieldWithRegEx(etlObject, fieldPath, regEx):
    filteredResults = etlObject.rawcol.find({ fieldPath: {"$regex" : regEx} })
    etlObject.mdb.insert(etlObject.regData["name"], filteredResults, "staging")
    pass