import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F
from ananta.Common import CustomSpark
from pydeequ.profiles import *
from ananta.Common.Functions import Helper, StringType, StructType, IntegerType, StructField
from ananta.DataReport.BaseReport import DataReport

__doc__ = """Generate Data Quality and duplicate, non qualify"""


class DataQuality(DataReport):
    """Generate Data Quality"""

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
                StructField("status", StringType(), True),
                StructField("duplicated", IntegerType(), True),
                StructField("notqualified", IntegerType(), True),
                StructField("raw", IntegerType(), True),
                StructField("transformed", IntegerType(), True),
                StructField("load", IntegerType(), True),
                StructField("uid", StringType(), True),
                StructField("file_ingest", StringType(), True),
                StructField("YYYY", StringType(), True),
                StructField("MM", StringType(), True),
                StructField("DD", StringType(), True),
            ]
        )

    def prepare_data(self, raw, transform, load, duplication, notqualify):
        """Prepare Data for dumping"""
        data = [
            {
                "created": self.landing_date,
                "layer": self.layer,
                "stage": self.stage,
                "country": self.country,
                "data_source": self.data_source,
                "object_name": self.object_name,
                "status": "completed",
                "uuid": self.uuid,
                "file_ingest": (
                    ",".join(self.file_path)
                    if isinstance(self.file_path, list)
                    else self.file_path
                ),
            }
        ]
        df = pd.read_json(json.dumps(data))
        df["duplicated"] = int(duplication)
        df["notqualified"] = int(notqualify)
        df["raw"] = int(raw)
        df["transformed"] = int(transform)
        df["load"] = int(load)
        df["YYYY"] = Helper.Datetime.convert_string_to_date(self.landing_date, fmt="%Y")
        df["MM"] = Helper.Datetime.convert_string_to_date(self.landing_date, fmt="%m")
        df["DD"] = Helper.Datetime.convert_string_to_date(self.landing_date, fmt="%d")
        return self.spark.createDataFrame(
            df[
                [
                    "created",
                    "layer",
                    "stage",
                    "country",
                    "data_source",
                    "object_name",
                    "status",
                    "duplicated",
                    "notqualified",
                    "raw",
                    "transformed",
                    "load",
                    "uuid",
                    "file_ingest",
                    "YYYY",
                    "MM",
                    "DD",
                ]
            ],
            schema=self.struct_schema,
        )

    def generate(self, raw, transform, load, duplication, notqualify):
        """Generate Data Quality"""
        data = self.prepare_data(
            raw=raw,
            transform=transform,
            load=load,
            duplication=duplication,
            notqualify=notqualify,
        )
        data.write.format("delta").mode("append").option(
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
            self.meta_data["path"]["data_quality"]
        )
        self.logger.log(
            f"DATA DataQuality: QUALITY: Generated with count as : {raw} - {transform}- {duplication}- {load}"
        )

    def generate_invalid_record(self, error_df):
        """Save Invalid record to file"""
        # TODO: Invalid column check from Data Contract
        error_df = error_df.withColumn("uuid", F.lit(self.uuid))
        path = f"""{self.meta_data['path']['data_quality'].replace("adls/", "")}{self.meta_data['path']['object_path']}{self.meta_data['path']['datetime_path']}{self.meta_data['path']['file_name'].replace('delta','parquet')}"""
        if self.debug:
            self.logger.log(path)
        error_df.write.format("parquet").mode("overwrite").option(
            "mergeSchema", True
        ).save(path)
