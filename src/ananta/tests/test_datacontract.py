from datetime import datetime

import pytest
from ananta.DataContract.Contract import UContract, UContractField


class TestDataContract:
    DATA_SOURCE = "beecost"
    OBJECT_NAME = "shopee_category"
    COUNTRY = "VN"
    NAME = f"{COUNTRY} Data beecost"
    DESCRIPTION = "test description lorem ipsum"
    DATA_OWNER = "email@example.com"
    PROCESS_OWNER = "email1@example.com"
    IS_ENABLED = True
    CREATED = datetime.now().isoformat()
    CRON = "0 8 * * */1"
    LAYER = "Bronze"
    STAGE = "Landed"
    contract = None

    def test_create_fields_structtype(self):
        fields = [
            UContractField(
                field_name="test_col", field_type="string", field_nullable=True
            ),
            UContractField(
                field_name="test_col", field_type="double", field_nullable=False
            ),
        ]
        assert len(fields) > 1
        return fields

    def test_init_create_contract(self):
        fields = self.test_create_fields_structtype()
        self.contract = UContract(
            contract_name=self.NAME,
            contract_description=self.DESCRIPTION,
            contract_data_source=self.DATA_SOURCE,
            contract_object_name=self.OBJECT_NAME,
            contract_data_owner=self.DATA_OWNER,
            contract_process_owner=self.PROCESS_OWNER,
            contract_is_enable=self.IS_ENABLED,
            contract_cron=self.CRON,
            contract_created_timestamp=self.CREATED,
            contract_fields=fields,
            contract_layer=self.LAYER,
            contract_stage=self.STAGE,
            contract_country=self.COUNTRY,
        )
        assert isinstance(self.contract, UContract)
