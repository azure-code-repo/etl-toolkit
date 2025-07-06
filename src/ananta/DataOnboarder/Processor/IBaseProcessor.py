from pyspark.sql import Window, DataFrame, SparkSession
from pyspark.sql import functions as F
from ananta.Common.Functions import Helper

from .BaseProcessor import Processor

__doc__ = """Implementation of Processor class"""


class BronzeLandedProcessor(Processor):
    """Bronze - Landed - Processor"""

    def __init__(self, **data):
        super().__init__(**data)
        self.file_reader = super().get_file_reader()

    def extract(self) -> DataFrame:
        reader = self.file_reader(**self.__dict__)
        self.input_df = reader.read()
        return self.input_df

    def transform(self, df) -> DataFrame:
        df = (
            df.withColumn("YYYY", F.lit(Helper.Datetime.get_current_datetime_now("%Y")))
            .withColumn("MM", F.lit(Helper.Datetime.get_current_datetime_now("%m")))
            .withColumn("DD", F.lit(Helper.Datetime.get_current_datetime_now("%d")))
        )
        self.transform_df = df
        error_df = SparkSession.getActiveSession().createDataFrame([], df.schema)
        error_df = error_df.withColumn("reason", F.lit(None).cast("string"))
        return self.transform_df, error_df

    def load(self, df):
        self.output_df = df
        # 30265 Multiple_file_path_support
        df.write.format("parquet").mode(self.save_mode).partitionBy(
            self.partition_by.split(",")
        ).save(self.meta_data["path"]["full_path"])
        return self.output_df


class BronzeProcessedProcessor(Processor):
    """Bronze - Processed - Processor"""

    def __init__(self, **data):
        super().__init__(**data)
        self.file_reader = super().get_file_reader()

    def extract(self) -> DataFrame:
        reader = self.file_reader(**self.__dict__)
        self.input_df = reader.read()
        return self.input_df

    def transform(self, df):
        if not df:
            raise ValueError(f"TRANSFORM DF: is not defined{self.__class__.__name__}")
        # pks
        transform_df = (
            df.withColumn(
                "InsertedTimestamp", F.lit(Helper.Datetime.get_current_timestamp_now())
            )
            .withColumn(
                "UpdatedTimestamp", F.lit(Helper.Datetime.get_current_timestamp_now())
            )
            .withColumn("YYYY", F.lit(Helper.Datetime.get_current_datetime_now("%Y")))
            .withColumn("MM", F.lit(Helper.Datetime.get_current_datetime_now("%m")))
            .withColumn("DD", F.lit(Helper.Datetime.get_current_datetime_now("%d")))
        )
        # self.transform_df = transform_df.dropDuplicates(
        # subset=self.meta_data["schema"]["primary_keys"]
        # )
        # 30425 Duplication Count
        window_partition = Window.partitionBy(
            self.meta_data["schema"]["primary_keys"]
        ).orderBy(F.lit(1))
        transform_df = transform_df.withColumn(
            "reason", F.row_number().over(window_partition)
        )
        error_df = transform_df.filter(F.col("reason") > 1).alias("duplication")
        error_df = error_df.withColumn("reason", F.lit("Duplication"))
        self.transform_df = (
            transform_df.filter(F.col("reason") == 1).drop().drop(F.col("reason"))
        )
        # 30424 Data Type Error Count
        for col in self.meta_data["schema"]["primary_keys"]:
            temp_df = transform_df.filter(F.col(col).isNull()).withColumn(
                "reason", F.lit(f"ValueError in Primary keys - {col}")
            )
            error_df = error_df.union(temp_df)
            transform_df = transform_df.filter(F.col(col).isNull()).drop()
        return self.transform_df, error_df

    def pre_load(self, df) -> None:
        """Preload for load process"""
        self.spark = SparkSession.builder.getOrCreate()
        schema_creation_sql = f"CREATE SCHEMA IF NOT EXISTS {self.meta_data['database']['schema']}"  # type: ignore
        self.spark.sql(schema_creation_sql)
        self.spark.sql(f'use {self.meta_data["database"]["schema"]}')  # type: ignore
        if self.debug:
            self.logger.log(
                f"Pre-load: PREP SCHEMA Completed! {self.meta_data['database']['schema']}"
            )
        target_schema_str = (
            self.spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(
                df.schema.json()
            ).toDDL()
        )
        table_creation_sql = f'''CREATE TABLE IF NOT EXISTS {self.meta_data['database']['table']} ({target_schema_str}) USING DELTA PARTITIONED BY ({self.meta_data['database'].get('partition_by','YYYY,MM,DD')}) LOCATION "{self.meta_data['path']['full_path']}"'''  # type: ignore
        if self.debug:
            self.logger.log(f"Pre-load: CREATE TABLE Completed! {table_creation_sql}")
        self.spark.sql(str(table_creation_sql))
        self.spark.catalog.dropTempView(self.meta_data["database"]["source_table"])  # type: ignore
        df.createOrReplaceTempView(f'{self.meta_data["database"]["source_table"]}')  # type: ignore

        self.meta_data["schema"]["join_string"] = " AND ".join(
            [
                f'COALESCE(Source.{col_name}, "NotAvailable") = COALESCE(Target.{col_name}, "NotAvailable")'
                for col_name in self.meta_data["schema"]["primary_keys"]
            ]
        )  # type: ignore
        final_cols = df.schema.names
        update_cols = list(
            filter(
                lambda v: v not in self.meta_data["schema"]["primary_keys"], final_cols
            )
        )
        self.meta_data["schema"]["update_string"] = ", ".join(
            [f"Target.{col_name} = Source.{col_name}" for col_name in update_cols]
        )  # type: ignore
        self.meta_data["schema"]["source_insert_string"] = ", ".join(
            [f"Source.{col_name}" for col_name in final_cols]
        )  # type: ignore
        self.meta_data["schema"]["target_insert_string"] = ", ".join(
            [f"Target.{col_name}" for col_name in final_cols]
        )  # type: ignore
        self.meta_data["schema"][
            "upsert_string"
        ] = f'MERGE INTO {self.meta_data["database"]["table"]} AS Target USING {self.meta_data["database"]["source_table"]} AS Source ON {self.meta_data["schema"]["join_string"]} WHEN MATCHED THEN UPDATE SET {self.meta_data["schema"]["update_string"]} WHEN NOT MATCHED THEN INSERT ({self.meta_data["schema"]["target_insert_string"]}) VALUES ({self.meta_data["schema"]["source_insert_string"]})'  # type: ignore
        if self.debug:
            self.logger.log(self.meta_data["schema"]["upsert_string"])  # type: ignore

    def load(self, df) -> DataFrame:
        self.pre_load(df)
        self.output_df = self.spark.sql(self.meta_data["schema"]["upsert_string"])  # type: ignore
        self.logger.log(f"Upsert complete to {self.meta_data['database']['table']}")  # type: ignore
        return df
