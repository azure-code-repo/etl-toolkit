import json

import pandas as pd
from pydeequ.profiles import ColumnProfilerRunner, StandardColumnProfile
from ananta.Common.Functions import (Helper, DoubleType, StringType,
                                     StructType, BooleanType, StructField)
from ananta.Common.CustomSpark import DataFrame, get_spark_session
from ananta.DataReport.BaseReport import DataReport

__doc__ = """Generate Data Catalog"""


class DataCatalog(DataReport):
    """Generate Data Catalog"""

    def __init__(self, **data):
        super().__init__(**data)
        if not self.meta_data:
            raise ValueError("No Meta Data generated. Contract available")
        self.spark = get_spark_session()
        self.struct_schema = StructType(
            [
                StructField("created", StringType(), True),
                StructField("layer", StringType(), True),
                StructField("stage", StringType(), True),
                StructField("country", StringType(), True),
                StructField("data_source", StringType(), True),
                StructField("object_name", StringType(), True),
                StructField("field_name", StringType(), True),
                StructField("data_type", StringType(), True),
                StructField("field_description", StringType(), True),
                StructField("field_validations", StringType(), True),
                StructField("field_primary", BooleanType(), True),
                StructField("nullable", StringType(), True),
                StructField("completeness", StringType(), True),
                StructField("distinct_value", StringType(), True),
                StructField("mean", DoubleType(), True),
                StructField("max", DoubleType(), True),
                StructField("min", DoubleType(), True),
                StructField("sum", DoubleType(), True),
                StructField("stdDev", DoubleType(), True),
                StructField("histogram", StringType(), True),
                StructField("uid", StringType(), True),
                StructField("file_ingest", StringType(), True),
                StructField("YYYY", StringType(), True),
                StructField("MM", StringType(), True),
                StructField("DD", StringType(), True),
            ]
        )

    def parse_result(self, result) -> DataFrame:
        """Parse result from the Column running"""
        list_result = []
        for col, p in zip(result.profiles, result.profiles.items()):
            col_dict = {
                "field_name_c": col.lower(),
                "dataType": p[1].dataType,
                "Nullable": "TRUE",
                "completeness": p[1]._completeness,
                "Distinct Value": p[1].approximateNumDistinctValues,
            }
            if isinstance(p[1], StandardColumnProfile):
                col_dict["Mean"] = None
                col_dict["Max"] = None
                col_dict["Min"] = None
                col_dict["Sum"] = None
                col_dict["StdDev"] = None
                col_dict["histogram"] = p[1].typeCounts
            else:
                col_dict["Mean"] = p[1]._mean
                col_dict["Max"] = p[1]._maximum
                col_dict["Min"] = p[1]._minimum
                col_dict["Sum"] = p[1]._sum
                col_dict["StdDev"] = p[1]._stdDev
                col_dict["histogram"] = p[1].histogram
            list_result.append(col_dict)
        result_df = pd.read_json(json.dumps(list_result))
        for col in ["Mean", "Max", "Min", "Sum", "StdDev"]:
            result_df[col] = result_df[col].astype(float)
        result_df["created"] = Helper.Datetime.get_current_datetime_now("%Y-%m-%d")
        result_df["data_source"] = self.data_source
        result_df["object_name"] = self.object_name
        result_df["uid"] = self.uuid
        year, month, day = (
            Helper.Datetime.convert_string_to_date(self.landing_date, fmt="%Y"),
            Helper.Datetime.convert_string_to_date(self.landing_date, fmt="%m"),
            Helper.Datetime.convert_string_to_date(self.landing_date, fmt="%d"),
        )
        result_df["YYYY"] = year
        result_df["MM"] = month
        result_df["DD"] = day
        result_df["file_ingest"] = (
            ",".join(self.file_path)
            if isinstance(self.file_path, list)
            else self.file_path
        )
        result_df["layer"] = self.layer
        result_df["stage"] = self.stage
        result_df["country"] = self.country
        contract_df = pd.json_normalize(
            self.contract.dict(),
            "contract_fields",
            [
                "contract_layer",
                "contract_stage",
                "contract_data_source",
                "contract_object_name",
                "contract_name",
                "contract_description",
                "contract_country",
            ],
        )
        result_df = result_df.merge(
            contract_df,
            how="left",
            left_on=[
                "data_source",
                "object_name",
                "layer",
                "stage",
                "country",
                "field_name_c",
            ],
            right_on=[
                "contract_data_source",
                "contract_object_name",
                "contract_layer",
                "contract_stage",
                "contract_country",
                "field_name",
            ],
        )
        # print(result_df.info())
        result_df["field_is_primary"] = (
            result_df["field_is_primary"].fillna(0).astype("bool")
        )
        return self.spark.createDataFrame(
            result_df[
                [
                    "created",
                    "layer",
                    "stage",
                    "country",
                    "data_source",
                    "object_name",
                    "field_name_c",
                    "dataType",
                    "field_description",
                    "field_validations",
                    "field_is_primary",
                    "Nullable",
                    "completeness",
                    "Distinct Value",
                    "Mean",
                    "Max",
                    "Min",
                    "Sum",
                    "StdDev",
                    "histogram",
                    "uid",
                    "file_ingest",
                    "YYYY",
                    "MM",
                    "DD",
                ]
            ],
            schema=self.struct_schema,
        )

    def generate(self, df):
        result = ColumnProfilerRunner(self.spark).onData(df).run()
        rdd = self.parse_result(result=result)
        rdd.write.format("delta").mode("append").option(
            "mergeSchema", True
        ).partitionBy(
            "layer",
            "data_source",
            "object_name",
            "country",
            "stage",
            "YYYY",
            "MM",
            "DD",
        ).save(
            self.meta_data["path"]["data_catalog"]
        )
        self.logger.log(
            f"DATA CATALOG: PROFILER: Generate {rdd.count()} rows match with {len(rdd.schema.names)} columns"
        )
