from abc import ABC
from typing import Any, Optional

from pydantic import Extra, BaseModel
from ananta.Common.CustomLogger import Logger


class DataReport(BaseModel, ABC):
    """Base Model for Data Reports"""

    country: str
    layer: str
    stage: str
    data_source: str
    object_name: str
    uuid: str
    landing_date: str
    save_mode: str
    object_type: str
    file_path: Optional[Any]
    meta_data: Optional[dict]
    output_df: Optional[Any]
    logger: Optional[Logger]
    debug: Optional[bool] = False
    raw: Optional[Any]
    transform: Optional[Any]
    load: Optional[Any]
    contract: Optional[Any]

    class Config:
        """Config class"""

        extra = Extra.allow
        allow_mutation = True

    def __init__(self, **data):
        super().__init__(**data)
        if not self.meta_data:
            raise ValueError("No Meta Data generated. Contract available")
