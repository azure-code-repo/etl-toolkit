from deltalake import DeltaTable

dt = DeltaTable("./build/DataQuality")
df = dt.to_pandas()
print(df.info())

dt2 = DeltaTable("./build/DataCatalogue")
df2 = dt2.to_pandas()
print(df2.info())
