import os
import re
import json
from typing import Any
from datetime import datetime

from dateutil import parser as date_praser
from pyspark.sql.types import (LongType, FloatType, DoubleType, StringType, StructType,
                               BooleanType, IntegerType, StructField, TimestampType)

__doc__ = """Helper common functions, all in one file"""


class ContractFunction:
    EMAIL_REGEX = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"

    @staticmethod
    def validate_email(email: str):
        """Validate email format from string"""
        if re.fullmatch(ContractFunction.EMAIL_REGEX, email):
            return email
        raise ValueError(f"Not an email: {email}")


class AnantaFunction:
    @staticmethod
    def is_local() -> bool:
        """Checking if local dev is enable

        :return: True or False depend on local dev
        :rtype: bool
        """
        return bool(os.environ.get("LOCAL_DEV", False))

    @staticmethod
    def get_processor(layer="Bronze", stage="Landed"):
        factory = {
            "Bronze": {
                "Landed": "BronzeLandedProcessor",
                "Processed": "BronzeProcessedProcessor",
            },
            "Silver": {
                "Landed": "SilverLandedProcessor",
                "Processed": "SilverProcessedProcessor",
            },
            "Other": {"Error": "NotFound"},
        }
        layer_factory = factory.get(layer, None)
        if layer_factory:
            stage_factory = layer_factory.get(stage, None)
            if stage_factory:
                return stage_factory
        return None


class Helper:
    """Helper utility accross the framework"""

    class Datetime:
        """Utility for date Time"""

        @staticmethod
        def get_current_datetime_now(fmt=None) -> Any:
            """
            Get current datetime

            :param fmt: string format datetime
            :type fmt: str
            :return: datetime else string fmt date
            """
            result = datetime.now()
            return result.strftime(fmt) if fmt else result

        @staticmethod
        def get_current_timestamp_now() -> Any:
            """
            Get current timestamp
            :return: isoformat
            """
            result = Helper.Datetime.get_current_datetime_now()
            return result.isoformat()

        @staticmethod
        def convert_string_to_date(string=None, **options) -> Any:
            """Convert String to datetime

            :param string: string date, defaults to None
            :type string: str, optional
            :param fnt: format String, defaults to None
            :type fmt: str, optional
            :return: datetime
            :rtype: datetime
            """
            if not string:
                return "Err"
            fmt = options.get("fmt", None)
            result = date_praser.parse(string)
            return result.strftime(fmt) if fmt else result

    class MetaData:
        """Metadata Utility"""

        @staticmethod
        def generate_json_to_schema_struct_type(json_schema):
            """Generate JSON dict into StructType"""
            schema_factory = {
                "Timestamp": TimestampType,
                "String": StringType,
                "Integer": IntegerType,
                "Long": LongType,
                "Boolean": BooleanType,
                "Double": DoubleType,
                "Float": FloatType,
            }
            schema_builder = []
            for item in json_schema:
                schema_item = StructField(
                    f'"{item}"', schema_factory[json_schema[item]](), True
                )
                schema_builder.append(schema_item)
            return StructType(schema_builder)

        @staticmethod
        def generate_metadata_for_runtime(
            country,
            stage,
            layer,
            data_source,
            object_name,
            object_type,
            landing_date,
            partition_by,
        ) -> dict:
            """_summary_

            :param country: country
            :type country: str
            :param stage: stage
            :type stage: str
            :param layer: layer
            :type layer: str
            :param data_source: data_source
            :type data_source: str
            :param object_name: object_name
            :type object_name: str
            :param object_type: object_type
            :type object_type: str
            :param landing_date: landing_date
            :type landing_date: str
            :return: metadata {'path':{},'database':{}}
            :rtype: dict
            """
            year, month, day = extract_datetime_to_y_m_d(landing_date)
            if bool(os.environ.get("LOCAL_DEV", False)):
                contract_path_prefix = str(
                    os.path.join(os.getcwd(), "data", "DataContract")
                )
                datalake_path = str(os.path.join(os.getcwd(), "build", "result")) + "/"
            else:
                datalake_path = f"/mnt/adls/"
                contract_path_prefix = f"/dbfs{datalake_path}DataContract"
            object_path = f"/{layer}/{data_source}/{object_name}/{country.upper()}/{stage.title()}"
            datetime_path = f"/YYYY={year}/MM={month}/DD={day}"
            file_name = f'/{object_name}_{str(convert_string_to_date(landing_date, fmt="%Y%m%d"))}.{object_type}'
            contract_path = f"{contract_path_prefix}/{layer}/{stage}/{country}/{data_source}_{object_name}.json"
            partition_by = (
                partition_by.lower() if partition_by != "YYYY,MM,DD" else partition_by
            )
            return {
                "path": {
                    "datalake_path": datalake_path,
                    "object_path": object_path,
                    "datetime_path": datetime_path,
                    "full_path": datalake_path
                    + object_path
                    + datetime_path
                    + file_name,
                    "processed_full_path": datalake_path + object_path,
                    "contract": contract_path,
                    "file_name": file_name,
                    "data_quality": f"{datalake_path}DataQuality",
                    "data_catalog": f"{datalake_path}DataCatalog",
                },
                "database": {
                    "schema": data_source.lower(),
                    "table": f"{data_source.lower()}.{object_name.lower()}",
                    "source_table": f"{object_name.lower()}_src",
                    "partition_by": partition_by,
                },
            }

    class File:
        """File Utility"""

        @staticmethod
        def get_date_from_file_name(file_name):
            """Get Date from file_name

            Example ern_shopee_best_selling_20221010.xlsx

            :param file_name: file name
            :type file_name: str
            :return: Date - 20221010
            :rtype: str
            """
            return file_name.split(".")[0].split("_")[-1]


# DATETIME UTILITY


def convert_string_to_date(string=None, **kwargs) -> Any:
    """Convert String to datetime"""
    if not string:
        return "Err"
    fmt = kwargs.get("fmt", None)
    result = date_praser.parse(string)
    return result.strftime(fmt) if fmt else result


def extract_datetime_to_y_m_d(date) -> Any:
    """Extract date as %Y %m %d"""
    date_convert = convert_string_to_date(date)
    return (
        date_convert.strftime("%Y"),
        date_convert.strftime("%m"),
        date_convert.strftime("%d"),
    )


# FILE NAME UTILITY


def get_current_date(**kwargs) -> Any:
    """Return current Date time

    :param fmt: format string, default '%Y-%m-%d'
    :type fmt: str
    :return: datetime
    :rtype: str
    """
    fmt = kwargs.get("fmt", "%Y-%m-%d")
    return datetime.now().strftime(fmt)


def get_current_timestamp() -> Any:
    """Get current date in isoformat"""
    return datetime.now().isoformat()


def get_date_from_file_name(file_name):
    """Extract datetime from file_name.

    Only work if date is last instance in file_name xxx_YYYMMDD.csv
    Args:

    :param file_name: file_name
    :type file_name: str
    :return: date string YYYMMDD
    :rtype: str
    """
    return file_name.split(".")[0].split("_")[-1]


def get_datetime_from_date(date, **kwargs):
    """_summary_

    :param date: string date
    :type date: str or datetime
    :param fmt: Optional Format string for datetime
    :type fmt: str
    :return: datetime
    :rtype: datetime
    """
    fmt = kwargs.get("fmt", None)
    if fmt:
        try:
            result = date_praser.parse(date).strftime(fmt)
        except ValueError:
            result = date
    else:
        result = date_praser.parse(date)
    return result


def get_date_as_Y_m_d(date, **kwargs):
    """Get d,m,Y as tuple

    Pass in date as string or format. The function will try to guess its date and return nicely

    :param date: string or formatted dated
    :type date: str or datetime
    :return: %Y, %m, %d
    :rtype: str
    """
    # Check exception if date is exists
    if not date or date == "null":
        return None
    # Check if date already datetime
    if isinstance(date, datetime):
        return date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")
    # Fuzzy matching format
    fmt = kwargs.get("fmt", None)
    if fmt:
        result = datetime.strftime(date, fmt)
    else:
        result = date_praser.parse(date)
    try:
        return result.strftime("%Y"), result.strftime("%m"), result.strftime("%d")
    except ValueError:
        return None
    except OverflowError:
        return None


if __name__ == "__main__":
    print(ContractFunction.validate_email("quandvdo@gmailcom"))
