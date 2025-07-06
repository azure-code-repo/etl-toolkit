from abc import ABC, abstractmethod
from typing import Any, Optional

from pydantic import Extra, BaseModel
from pyspark.sql import SparkSession


class FileReader(BaseModel, ABC):
    """Base Model class for File Reader"""

    country: str
    layer: str
    stage: str
    data_source: str
    object_name: str
    file_path: Optional[Any]
    landing_date: str
    save_mode: str
    object_type: str
    meta_data: Optional[dict]
    debug: Optional[bool] = False

    class Config:
        """Config class"""

        extra = Extra.allow

    def __init__(self, **data):
        super().__init__(**data)
        self.spark = SparkSession.getActiveSession()

    @abstractmethod
    def prepare(self, **options):
        """prepare data to ingest"""
        pass

    @abstractmethod
    def read(self, **options):
        """Read Function"""
        pass
