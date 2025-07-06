import os
import sys

if os.environ.get("LOCAL_DEV", False):
    sys.path.insert(0, "C:/Users/PtPashantTripathi/code/ananta/src")
from ananta.core import Ananta
from ananta.Common import Messages, Functions, CustomLogger
from ananta.DataOnboarder.Processor import BaseProcessor, IBaseProcessor
from ananta.DataOnboarder.Processor.IFileReader import CsvFileReader


def print_var(value):
    print(f"---------\n{value}\n-----------")


DATA_SOURCE = "beecost"
OBJECT_NAME = "shopee_category_test"
COUNTRY = "VN"
LAYER = "BRONZE"
STAGE = "Landed"
NAME = f"{COUNTRY} Data beecost"
DESCRIPTION = "test description lorem ipsum"
# DATA_OWNER = "email@example.com"
# PROCESS_OWNER = "email1@example.com"
IS_ENABLED = True
CRON = "0 8 * * */1"
FILE_PATH = "C:/Soft/test_20221001.csv"
SAVE_MODE = "overwrite"
DEBUG = True
OBJECT_TYPE = "parquet"
LANDING_DATE = "2022-10-10"

meta_data = Functions.Helper.MetaData.generate_metadata_for_runtime(
    country=COUNTRY,
    stage=STAGE,
    layer=LAYER,
    data_source=DATA_SOURCE,
    object_name=OBJECT_NAME,
    object_type=OBJECT_TYPE,
    landing_date=LANDING_DATE,
)

app = IBaseProcessor.BronzeLandedProcessor(
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
    meta_data=meta_data,
    logger=CustomLogger.Logger(),
)
print(app.file_path)
df = app.extract()
print(df)
