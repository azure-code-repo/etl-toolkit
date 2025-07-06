import os
import sys
from sys import platform

from pyspark.sql import functions as F

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
from ananta.Common import Messages, CustomLogger


def print_var(value):
    print(f"---------\n{value}\n-----------")


DATA_SOURCE = "ESS"
OBJECT_NAME = "rptOOS"
COUNTRY = "VN"
LAYER = "BRONZE"
STAGE = "Processed"
NAME = f"{COUNTRY} Data beecost"
DESCRIPTION = "test description lorem ipsum"
# DATA_OWNER = "email@example.com"
# PROCESS_OWNER = "email1@example.com"
IS_ENABLED = True
CRON = "0 8 * * */1"
# FILE_PATH = "./build/result/Beecost/shopee_category_test/VN/Landed/YYYY=2022/MM=10/DD=10/shopee_category_test_20221010.parquet"
FILE_PATH = None
SAVE_MODE = "overwrite"
DEBUG = True
OBJECT_TYPE = "delta"
LANDING_DATE = "2022-10-10"
PARTITION_BY = "segment,average_rating"

# app = Ananta(
#     country=COUNTRY,
#     layer=LAYER,
#     stage=STAGE,
#     data_source=DATA_SOURCE,
#     object_name=OBJECT_NAME,
#     file_path=FILE_PATH,
#     save_mode=SAVE_MODE,
#     object_type=OBJECT_TYPE,
#     debug=DEBUG,
#     landing_date=LANDING_DATE,
# )
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


# def custom_function():
#     return F.concat(
#         "seller_id",
#         F.lit("_"),
#         "product_id",
#         F.lit("_"),
#         "model_id",
#         F.lit("_"),
#         F.date_format(F.col("datetime"), "yyyyMMdd"),
#     )


# app.add_custom_udf("uid", custom_function, "POST_EXTRACT", None)
# app.add_custom_udf("colB", "custom_function2", "PRE_TRANSFORM", None)
# app.add_custom_udf("colC", "custom_function3", "POST_LOAD", None)
app.run()
# app.custom_udf(function_name, "extract/transform/load")  # pre/post
# print(f"-----Result {app.input_df.count()}")  # type: ignore
