import os
import json
from typing import Any, Optional

from pydantic import Extra, BaseModel
from ananta.Common import Functions, CustomLogger
from ananta.DataContract.Contract import UContract

__doc__ = """Contract Broker for Base Model"""


class ContractBroker(BaseModel):
    """Broker to look for contracts"""

    layer: str
    stage: str
    data_source: str
    object_name: str
    file_path: Optional[Any]
    override_path: Optional[str] = None
    is_local: Optional[bool] = False
    debug: Optional[bool] = False
    meta_data: Optional[dict]
    contract: Optional[UContract] = None
    logger: Optional[CustomLogger.Logger] = None

    class Config:
        """Base Model Config"""

        extra = Extra.allow
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        self.is_local = bool(Functions.AnantaFunction.is_local())

    def parse_contract_from_path(self, path) -> UContract:
        """Parse and return Contract"""
        clean_path = self._cleansing_path_for_os_use(path)
        with open(clean_path, encoding="utf-8") as file:
            json_data = json.load(file)
        contract = UContract(**json_data)
        return contract

    def _cleansing_path_for_os_use(self, path) -> str:
        return f"/dbfs{path}" if not "/dbfs" in path and not self.is_local else path

    def check_deployment_folder(self) -> Any:
        """Checking deployment Folder at /mnt/adls/DataContract/{self.layer}/{self.data_source}/{self.object_name}/{self.country}/{self.stage}/

        :raises ValueError: _description_
        :return: _description_
        :rtype: _type_
        """
        if self.debug:
            self.logger.log(self.meta_data["path"]["contract"])
        if not bool(os.path.exists(self.meta_data["path"]["contract"])):
            self.logger.log(
                f"CONTRACT: BROKER: No Contract Found for {self.meta_data['path']['contract']}! Please Generate!"
            )
            return None
        self.logger.log(
            f"CONTRACT: BROKER: Found Contract at {self.meta_data['path']['contract']}"
        )
        return self.parse_contract_from_path(self.meta_data["path"]["contract"])
