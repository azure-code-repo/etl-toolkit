import uuid

import pandas as pd
from ananta.Common import CustomSpark
from pydeequ.profiles import *
from ananta.Common.Functions import *
from ananta.DataReport.BaseReport import DataReport


class DataProfiler(DataReport):
    def __init__(self, **data):
        super().__init__(**data)
        if not self.meta_data:
            raise ValueError("No Meta Data generated. Contract available")
        self.spark = CustomSpark.get_spark_session()
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
                StructField("YYYY", StringType(), True),
                StructField("MM", StringType(), True),
                StructField("DD", StringType(), True),
            ]
        )

    def parse_result(self, result) -> DataFrame:
        list_result = []
        for col, p in zip(result.profiles, result.profiles.items()):
            col_dict = {
                "field_name": col,
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
        result_df["uid"] = result_df["created"].apply(lambda x: str(uuid.uuid4()))
        year, month, day = extract_datetime_to_y_m_d(self.landing_date)
        result_df["YYYY"] = year
        result_df["MM"] = month
        result_df["DD"] = day
        result_df["layer"] = self.layer
        result_df["stage"] = self.stage
        result_df["country"] = self.country
        # print(result_df.info())
        return self.spark.createDataFrame(
            result_df[
                [
                    "created",
                    "layer",
                    "stage",
                    "country",
                    "data_source",
                    "object_name",
                    "field_name",
                    "dataType",
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
        path = f"/mnt/adls/DataCatalogue/"
        if os.environ.get("LOCAL_DEV", False):
            path = path.replace(
                "/mnt/adls/DataCatalogue", f"{os.getcwd()}/build/DataCatalogue"
            )
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
            path
        )
        self.logger.log(
            f"DATA DataQuality: PROFILER: Generate {rdd.count()} rows match with {len(rdd.schema.names)} columns"
        )
