import inspect
from abc import ABC, abstractmethod
from typing import Any, Optional

from pydantic import Extra, BaseModel
from ananta.Common.CustomLogger import Logger
from ananta.DataOnboarder.Processor import FileReader, IFileReader

__doc__ = """
Base Model for Processor
"""


class Processor(BaseModel, ABC):
    """Base Process class for different layer and stage

    :param layer: _description_
    :type layer: str
    :param stage: _description_
    :type stage: str
    :param file_path: _description_
    :type file_path: str
    :param landing_date: _description_
    :type landing_date: str
    """

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
    logger: Optional[Logger]
    debug: Optional[bool] = False
    partition_by: Optional[str]

    class Config:
        extra = Extra.allow
        allow_mutation = True

    def __init__(self, **data):
        super().__init__(**data)
        if not self.meta_data:
            raise ValueError("No Meta Data generated. Contract available")

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self, df):
        pass

    @abstractmethod
    def load(self, df):
        pass

    def get_file_reader(self):
        ext_pos = -2 if ".gz" in self.file_path else -1
        if isinstance(self.file_path, list):
            ext = [
                single_file.split(".")[ext_pos].replace(".", "")
                for single_file in self.file_path
            ][0]
        else:
            ext = self.file_path.split(".")[ext_pos].replace(".", "")
        self.meta_data["file"] = {"ext": ext}
        file_reader_factory = {
            "csv": IFileReader.CsvFileReader,
            "tsv": IFileReader.CsvFileReader,
            "xlsx": IFileReader.ExcelFileReader,
            "xls": IFileReader.ExcelFileReader,
            "parquet": IFileReader.ParquetFileReader,
        }
        return file_reader_factory[ext]

    def generate_primary_keys_column(self, df):
        pks = self.meta_data["schema"].get("primary_keys", None)
        if len(pks) == 1:
            return pks[0]
        return pks
