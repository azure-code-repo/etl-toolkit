from uuid import uuid4
from typing import Any, Optional

from pydantic import Extra, Field, BaseModel, validator
from pyspark.sql import DataFrame
from ananta.Common import Constants, Functions, CustomLogger
from ananta.core_error import NoContractFound
from ananta.DataReport.Catalog import DataCatalog
from ananta.DataReport.Quality import DataQuality
from ananta.DataContract.Broker import ContractBroker
from ananta.DataOnboarder.Processor import IBaseProcessor

__version__ = "0.2.44"
# module level doc-string
__doc__ = """
Ananta - Ananta Data On-Boarders
=====================================================================

**Ananta** is a Python package providing fast, flexible, and expressive data
structures designed to make working with "relational" or "labeled" data both
easy and intuitive. It aims to be the fundamental high-level building block for
doing practical, **real world** data analysis in Python. Additionally, it has
the broader goal of becoming **the most powerful and flexible open source data
analysis / manipulation tool available in any language**. It is already well on
its way toward this goal.

Main Features
-------------
Here are just a few of the things that Ananta does well:

    - Easy handling of missing data in floating point as well as non-floating
      point data.
    - Size mutability: columns can be inserted and deleted from DataFrame and
      higher dimensional objects
    - Automatic and explicit data alignment: objects can be explicitly aligned
      to a set of labels, or the user can simply ignore the labels and let
      `Series`, `DataFrame`, etc. automatically align the data for you in
      computations.
    - Powerful, flexible group by functionality to perform split-apply-combine
      operations on data sets, for both aggregating and transforming data.
    - Make it easy to convert ragged, differently-indexed data in other Python
      and NumPy data structures into DataFrame objects.
    - Intelligent label-based slicing, fancy indexing, and subsetting of large
      data sets.
    - Intuitive merging and joining data sets.
    - Flexible reshaping and pivoting of data sets.
    - Hierarchical labeling of axes (possible to have multiple labels per tick).
    - Robust IO tools for loading data from flat files (CSV and delimited),
      Excel files, databases, and saving/loading data from the ultrafast HDF5
      format.
    - Time series-specific functionality: date range generation and frequency
      conversion, moving window statistics, date shifting and lagging.
"""


class Ananta(BaseModel):
    """Ananta Data On-Boarder Framework main function.

    :param country: Country of the job instance
    :type country: str
    :param layer: Layer of the job instance
    :type layer: str
    :param Stage: Stage of the job instance
    :type Stage: str
    :param data_source: data_source of the job instance
    :type data_source: str
    :param object_name: object_name of the job instance
    :type object_name: str
    :param landing_date: landing_date of the job instance
    :type landing_date: str
    :param save_mode: save_mode of the job instance ```'overwrite' 'error' 'append' 'ignore'```.
    Defaults to error
    :type save_mode: str
    :param object_type: object_type of the job instance. i.e ```'parquet' 'delta'```
    :type object_type: str
    :param debug: debug runtime. Display more information. Defaults to
    :type debug: bool
    :return: Ananta framework
    :rtype: Ananta
    """

    country: str
    layer: str
    stage: str
    data_source: str
    object_name: str
    file_path: Optional[Any]
    uuid: Optional[str] = Field(default=uuid4())
    landing_date: str
    save_mode: str
    object_type: str
    partition_by: Optional[str]
    debug: Optional[bool] = False
    logger: Optional[CustomLogger.Logger] = None
    custom_steps: Optional[dict] = Field(
        default={
            "PRE_EXTRACT": [],
            "POST_EXTRACT": [],
            "PRE_TRANSFORM": [],
            "POST_TRANSFORM": [],
            "PRE_LOAD": [],
            "POST_LOAD": [],
        }
    )
    input_df: Optional[DataFrame] = None
    transform_df: Optional[DataFrame] = None
    output_df: Optional[DataFrame] = None
    error_df: Optional[DataFrame] = None
    dq_df: Optional[DataFrame] = None
    data_catalog: Optional[bool] = True
    report: Optional[dict] = Field(
        default={
            "RAW": 0,
            "TRANSFORM": 0,
            "DUPLICATION": 0,
            "LOAD": 0,
            "NOTQUALIFIED": 0,
        }
    )

    class Config:
        """Config of Pydantic base class"""

        arbitrary_types_allowed = True
        extra = Extra.allow
        allow_mutation = True

    def __init__(self, **data):
        super().__init__(**data)
        self.logger = CustomLogger.Logger()
        self.logger.log("Initialized Ananta")
        self.custom_steps_count = 0
        if not self.partition_by:
            self.partition_by = "YYYY,MM,DD"
        self.meta_data = Functions.Helper.MetaData.generate_metadata_for_runtime(
            country=self.country,
            stage=self.stage,
            layer=self.layer,
            data_source=self.data_source,
            object_name=self.object_name,
            object_type=self.object_type,
            landing_date=self.landing_date,
            partition_by=self.partition_by,
        )
        if not self.file_path:
            self.file_path = f"""{self.meta_data["path"]["full_path"].replace('/Processed','/Landed').replace('delta','parquet')}"""
            if self.debug:
                self.logger.log(f"DEBUG: NO file_path : looking new {self.file_path}")
        self.payload = {
            "country": self.country,
            "layer": self.layer,
            "stage": self.stage,
            "data_source": self.data_source,
            "object_name": self.object_name,
            "file_path": self.file_path,
            "object_type": self.object_type,
            "save_mode": self.save_mode,
            "landing_date": self.landing_date,
            "debug": self.debug,
            "meta_data": self.meta_data,
            "logger": self.logger,
            "partition_by": self.partition_by,
            "uuid": str(self.uuid),
        }
        broker = ContractBroker(**self.payload)
        self.contract = broker.check_deployment_folder()
        self.payload["contract"] = self.contract
        self.processor = self.assign_processor()

    # VALIDATIONS

    @validator("country")
    def validator_country(cls, value):
        """Validation for country must not be null"""
        if value == "SEAID":
            raise ValueError(
                "Country is set to SEAID. Something not right, perhaps Country has not been set!"
            )
        if not Constants.Country.__contains__(value.upper()):
            raise ValueError(
                "Country is not configured to run Ananta. Please contact support at PtPashantTripathi@outlook.com"
            )
        return value.upper()

    @validator("layer")
    def validator_layer(cls, value):
        """Validation for layer must not be null"""
        if not Constants.Layer.__contains__(value.lower().title()):
            raise ValueError(
                "Layer is not configured to run Ananta. Please contact support at PtPashantTripathi@outlook.com"
            )
        return value.lower().title()

    @validator("stage")
    def validator_stage(cls, value):
        """Validation for stage must not be null"""
        if not Constants.Stage.__contains__(value.lower().title()):
            raise ValueError(
                "Stage is not configured to run Ananta. Please contact support at PtPashantTripathi@outlook.com"
            )
        return value.lower().title()

    @validator("data_source")
    def validator_data_source(cls, value):
        """Validation for data_source"""
        if not value:
            raise ValueError("Data Source is null. Please fill this in.")
        return value

    @validator("object_name")
    def validator_object_name(cls, value):
        """Validation for object_name"""
        if not value:
            raise ValueError("object_name is null. Please fill this in.")
        return value

    # 30266: Adding partition_by validation
    @validator("partition_by")
    def validator_partition_by(cls, value):
        """Validation for object_name"""
        if not value:
            return "YYYY,MM,DD"
        if "YYYY,MM,DD" in value:
            return value
        return f"{value},YYYY,MM,DD"

    # 30265 : Multiple_file_path_support
    @validator("file_path")
    def validator_file_path(cls, value):
        """Validation for file_path"""
        # if self.file_path and self.file_path.split("/")[0] == "processing":
        #     self.file_path = "/mnt/" + self.file_path
        # if not self.file_path:
        #     self.file_path = f"""{self.meta_data["path"]["full_path"].replace('/Processed','/Landed')}{self.meta_data['path']['datetime_path']}{self.meta_data['path']['file_name'].replace(self.object_type,'parquet')}"""
        #     self.logger.log(f"DEBUG: NO file_path : looking new {self.file_path}")
        if not value:
            return None
        if "processing/" in value:
            value = f"/mnt/{value}"
        if "," in value:
            value = value.split(",")
        return value

    def add_custom_udf(self, col_name: str, function, location: str, params):
        """Allow user to add custom udf with location before or after ETL step

        :param col_name: column name
        :type col_name: str
        :param function: callable function
        :type function: str
        :param location: ```PRE/POST_EXTRACT,
        PRE/POST_TRANSFORM,
        PRE/POST_LOAD```
        :type location: str
        :param params: params
        :type params: Any
        """
        if not Constants.UdfLocation.__contains__(location.upper()):
            raise ValueError(f"CORE: Invalid location indicator {location}")
        self.custom_steps[location.upper()].append(
            {"col_name": col_name, "function_name": function, "params": params}
        )  # type: ignore
        self.custom_steps_count += 1

    def execute_custom_udf(self, location) -> DataFrame:
        """Assign Ananta DataFrame holder to correct stage and execute custom udf"""
        loc = location.split("_")[-1]
        factory = {
            "EXTRACT": "input_df",
            "TRANSFORM": "transform_df",
            "LOAD": "output_df",
        }
        select_df_name = factory.get(loc, None)
        if not select_df_name:
            raise ValueError(f"ERROR: {loc} is not found")
        select_df = getattr(self, select_df_name)
        if len(self.custom_steps[location]) > 0:  # type: ignore
            for step in self.custom_steps[location]:  # type: ignore
                self.logger.log(
                    f"CORE: custom - {location} - {step['col_name']} - {step['function_name']} - {step['params']}"
                )  # type: ignore
                select_df = select_df.withColumn(
                    step["col_name"], step["function_name"]()
                )
        return select_df

    def check_contract(self):
        """Create Contract Broker and Looking for contract in designated location"""
        if not self.contract:
            raise NoContractFound(
                f"ERROR: NO CONTRACT FOUND! {self.meta_data['path']['contract']}"
            )
        self.meta_data["schema"] = {
            "struct": self.contract.generate_struct_schema(),
            "root": self.contract.generate_hive_schema(),
            "primary_keys": self.contract.generate_primary_keys(),
        }

    def assign_processor(self) -> IBaseProcessor:
        """Assigning Layer/Stage Processor"""
        processor_name = Functions.AnantaFunction.get_processor(self.layer, self.stage)
        if not processor_name:
            raise ValueError(
                f"Processor has not been setup for {self.layer} - {self.stage}"
            )
        processor = getattr(IBaseProcessor, processor_name)(**self.payload)
        if not processor:
            raise ValueError(
                f"{self.__class__.__name__} : Error in getting up processor"
            )
        self.logger.log(f"ASSIGNED: {processor.__class__.__name__}")
        return processor

    def generate_data_quality_report(self):
        """Generate Data Quality Report"""
        data_quality = DataQuality(**self.payload)
        data_quality.generate(
            raw=self.input_df.count(),
            transform=self.transform_df.count(),
            load=self.output_df.count(),
            duplication=self.error_df.filter("reason=='Duplication'").count(),
            notqualify=self.error_df.filter("reason!='Duplication'").count(),
        )
        data_quality.generate_invalid_record(self.error_df)
        self.logger.log("DATA QUALITY: QUALITY: Complete")

    def generate_data_catalog_report(self):
        """Generate Data Catalog"""
        data_profiler = DataCatalog(**self.payload)
        data_profiler.generate(df=self.output_df)
        self.logger.log("DATA CATALOG: PROFILER: Complete")

    def add_runtime_report(self, step, df) -> None:
        """Adding result to report placeholder"""
        self.report[step] = df.count()
        self.logger.log(
            f"{self.stage.upper()} {step.upper()} COMPLETED: {self.report[step]}"
        )
        self.payload[step.lower()] = df

    def execute(self):
        """Execute Steps"""
        # if self.debug:
        #    self.logger.log(str(self.meta_data))  # type: ignore
        self.logger.log(f"CORE: Found {self.custom_steps_count} custom udf")  # type: ignore
        # Extract
        self.input_df = self.processor.extract()
        self.input_df = self.execute_custom_udf("POST_EXTRACT")
        self.add_runtime_report("RAW", self.input_df)
        # Transform
        self.transform_df, self.error_df = self.processor.transform(self.input_df)
        self.transform_df = self.execute_custom_udf("POST_TRANSFORM")
        self.add_runtime_report(
            "DUPLICATION", self.error_df.filter("reason=='Duplication'")
        )
        self.add_runtime_report(
            "NOTQUALIFIED", self.error_df.filter("reason!='Duplication'")
        )
        self.add_runtime_report("TRANSFORM", self.transform_df)
        # Load
        self.output_df = self.processor.load(self.transform_df)
        self.output_df = self.execute_custom_udf("POST_LOAD")
        self.add_runtime_report("LOAD", self.output_df)

    def log_runtime_report(self) -> None:
        """Logging runtime"""
        for index, value in self.report.items():
            self.logger.log(f"{index.upper()}:{value}")

    def get_runtime_report(self) -> dict:
        """Return Run time Report as dict {RAW:count(), TRANSFORM:count(), LOAD:count(), DUPLICATION:count(), NOTQUALIFIED:count()}"""
        return self.report

    def run(self):
        """Run itself"""
        if self.debug and isintance(self.file_path, list):
            self.logger.log(",".join(self.file_path))
        self.check_contract()
        self.execute()
        # self.log_runtime_report()
        self.generate_data_quality_report()
        if self.data_catalog:
            self.generate_data_catalog_report()
