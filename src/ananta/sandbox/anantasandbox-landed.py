import os
import sys
from sys import platform

import pandas as pd
from pyspark.sql import functions as F

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

from ananta.core import Ananta

DATA_SOURCE = "ESS"
OBJECT_NAME = "rptOOS"
COUNTRY = "VN"
LAYER = "BRONZE"
STAGE = "Landed"
IS_ENABLED = True
FILE_PATH = (
    "C:/Soft/test_20221001.csv"
    if platform == "win32"
    else "./rptOOS_20220921.parquet,./rptOOS_20220921_2.parquet"
)
SAVE_MODE = "overwrite"
DEBUG = True
OBJECT_TYPE = "parquet"
LANDING_DATE = "2022-10-10"
print(FILE_PATH)
app = Ananta(
    country=COUNTRY,
    layer=LAYER,
    stage=STAGE,
    data_source=DATA_SOURCE,
    object_name=OBJECT_NAME,
    file_path=FILE_PATH,
    save_mode=SAVE_MODE,
    object_type=OBJECT_TYPE,
    debug=DEBUG,
    landing_date=LANDING_DATE,
)
# def custom_function(variable):
#     return F.udf()
# app.add_custom_udf("colA", "custom_function", "POST_EXTRACT", None)
# app.add_custom_udf("colB", "custom_function2", "PRE_TRANSFORM", None)
# app.add_custom_udf("colC", "custom_function3", "POST_LOAD", None)
## app.run()
#   # print(app.meta_data)
#   #print("PAYLOAD - ", app.payload)
## app.custom_udf(function_name, "extract/transform/load")  # pre/post
## print_var(app.processor.dict())
# print(app.contract.to_json())
# df = pd.DataFrame(app.contract.dict())
# print(df.head())
df = pd.json_normalize(
    app.contract.dict(),
    "contract_fields",
    [
        "contract_layer",
        "contract_stage",
        "contract_data_source",
        "contract_object_name",
        "contract_name",
        "contract_description",
    ],
)
df["field_is_primary"] = df["field_is_primary"].fillna(0).astype("bool")
print(df.head())
