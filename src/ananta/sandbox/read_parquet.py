import os
import sys
import json
from sys import platform
from glob import glob

import pandas as pd

print(platform)
if os.environ.get("LOCAL_DEV", False):
    if platform == "linux" or platform == "linux2":
        # linux
        pass
    elif platform == "darwin":
        print(f"path added")
        sys.path.insert(0, "/Users/kom/code/ananta/src")

    elif platform == "win32":
        sys.path.insert(0, "C:/Users/PtPashantTripathi/code/ananta/src")
import ananta
from ananta.Common import CustomSpark
from ananta.utilities import CreateContract

PATH = os.getcwd()
parquet_files = glob(f"{PATH}/*.parquet")
print(parquet_files)
spark = CustomSpark.get_spark_session()
# print(parquet_files)
# for file in parquet_files:
# print(file)
# df = pd.read_parquet(f"{file}")
#  print(f"{file}", df.columns, sep="-")
# print(df.info())
# new_df = spark.createDataFrame(df)
#   dup_df = df.loc[range(0, 10)]
#    df = df.append(dup_df, ignore_index=True)
# df.to_parquet(file)

# from pyspark.sql import *
# from pyspark.sql import functions as F

# w = Window.partitionBy('transactiondate', 'storecode', 'dpcode', 'reasoncode', 'csecode').orderBy(F.lit(1))
# print(parquet_files)
# df = spark.read.format('parquet').parquet(*parquet_files)
# print('before',df.count())
# df = df.withColumn('reason', F.row_number().over(w))
# dup_df = df.filter(F.col('reason')>1).alias('dup_df')
# df = df.filter(F.col('reason')==1).drop()
# print('after ',df.count())
# print('duplicate',dup_df.count())
# df = spark.read.format("parquet").parquet(
#     "./build/result/DataQuality/Bronze/ESS/rptOOS/VN/Processed/YYYY=2022/MM=10/DD=10/rptOOS_20221010.parquet"
# )
# print(df.printSchema())
df = spark.read.format("delta").load("./build/result/DataCatalog")
print(df.printSchema())
print(df.take(10))
