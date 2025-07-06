import os
import sys

if os.environ.get("LOCAL_DEV", False):
    sys.path.insert(0, "C:/Users/PtPashantTripathi/code/ananta")
from ananta.Common import *
from pydantic.schema import schema
from ananta.DataContract.Broker import ContractBroker
from ananta.DataContract.Contract import UContract, UContractField

# print(Constants.Country.__contains__("VN"))


def print_var(var):
    print("-------")
    print(var)
    print("-------")


DATA_SOURCE = "beecost"
OBJECT_NAME = "shopee_category_test"
COUNTRY = "VN"
LAYER = "Bronze"
STAGE = "Landed"
NAME = f"{COUNTRY} Data beecost"
DESCRIPTION = "test description lorem ipsum"
DATA_OWNER = "email@example.com"
PROCESS_OWNER = "email1@example.com"
IS_ENABLED = True
CRON = "0 8 * * */1"

# SCHEMA_BEECOST = StructType(
#     [
#         StructField("datetime", TimestampType(), True),
#         StructField("category_l1", StringType(), True),
#         StructField("category_l2", StringType(), True),
#         StructField("category_l3", StringType(), True),
#         StructField("segment", StringType(), True),
#         StructField("title", StringType(), True),
#         StructField("description", StringType(), True),
#         StructField("brand", StringType(), True),
#         StructField("product_id", LongType(), True),
#         StructField("model_id", LongType(), True),
#         StructField("model_name", StringType(), True),
#         StructField("offer_price", DoubleType(), True),
#         StructField("sale_price", DoubleType(), True),
#         StructField("units_sold_total", IntegerType(), True),
#         StructField("units_sold_variant", StringType(), True),
#         StructField("units_sold_listing_page", IntegerType(), True),
#         StructField("product_url", StringType(), True),
#         StructField("average_rating", DoubleType(), True),
#         StructField("number_rating", IntegerType(), True),
#         StructField("seller_id", StringType(), True),
#         StructField("seller_name", StringType(), True),
#         StructField("number_review_with_comment", IntegerType(), True),
#         StructField("number_review_with_image_video", IntegerType(), True),
#         StructField("breadcrum", StringType(), True),
#         StructField("mall_product", BooleanType(), True),
#         StructField("volume", StringType(), True),
#         StructField("weight", StringType(), True),
#         StructField("is_flashsale", BooleanType(), True),
#         StructField("main_image_url", StringType(), True),
#         StructField("alternate_image_1", StringType(), True),
#         StructField("alternate_image_2", StringType(), True),
#         StructField("seller_coupon", StringType(), True),
#         StructField("numbe_promo_total", IntegerType(), True),
#         StructField("numbe_promo_variant", IntegerType(), True),
#         StructField("availability", BooleanType(), True),
#         StructField("specification_send_from", StringType(), True),
#         StructField("inventory", IntegerType(), True),
#         StructField("seller_since", TimestampType(), True),
#         StructField("abnormal_product", BooleanType(), True),
#         StructField("unit_sold_30_days", LongType(), True),
#         StructField("unit_sold_90_days", LongType(), True),
#         StructField("unit_sold_180_days", LongType(), True),
#         StructField("unit_sold_365_days", LongType(), True),
#         StructField("revenue_30_days", LongType(), True),
#         StructField("revenue_90_days", LongType(), True),
#         StructField("revenue_180_days", LongType(), True),
#         StructField("revenue_365_days", LongType(), True),
#     ]
# )
# fields_list = []

# factory = {
#     "TimestampType": "Timestamp",
#     "StringType": "String",
#     "IntegerType": "Integer",
#     "LongType": "Long",
#     "BooleanType": "Boolean",
#     "DoubleType": "Double",
# }

# for field in SCHEMA_BEECOST:
#     field_item = UContractField(
#         field_name=field.name,
#         field_type=factory[str(field.dataType)],
#         field_nullable=field.nullable,
#     )
#     fields_list.append(field_item)

# # print(fields_list)

# # fields_1 = {
# #     "field_name": "tesrt_col",
# #     "field_type": "string",
# #     "field_nullable": True,
# #     "field_is_primary": True,
# # }
# # fields_2 = {"field_name": "test_col", "field_type": "double", "field_nullable": False}
# contract_data = {
#     "contract_name": NAME,
#     "contract_description": DESCRIPTION,
#     "contract_data_source": DATA_SOURCE,
#     "contract_object_name": OBJECT_NAME,
#     "contract_country": COUNTRY,
#     "contract_layer": LAYER,
#     "contract_stage": STAGE,
#     "contract_data_owner": DATA_OWNER,
#     "contract_process_owner": PROCESS_OWNER,
#     "contract_is_enable": IS_ENABLED,
#     "contract_cron": CRON,
#     "contract_fields": fields_list,
# }
# # # print_var(UContractField(**fields_1))
# # # print_var(contract_data)
# contract = UContract(**contract_data)
# print(contract.json())r

# # print(contract.generate_schema())

broker = ContractBroker(
    layer=LAYER, stage=STAGE, data_source=DATA_SOURCE, object_name=OBJECT_NAME
)
contract = broker.check_deployment_folder()

# # print(result)
# # print_var(contract.generate_struct_schema())
# # print_var(contract.generate_hive_schema())
# print(contract)
# print_var(contract.generate_primary_keys())
print_var(contract)
