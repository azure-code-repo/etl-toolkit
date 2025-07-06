import os

import pytest
from ananta.DataContract.Broker import ContractBroker
from ananta.DataContract.Contract import UContract, UContractField


class TestDataContractBroker:
    TEST_JSON_PATH = f"{os.getcwd()}/src/ananta/tests/data/DataContract/Bronze/Layer/Beecost/shopee_category_test.json"
    DATA_SOURCE = "beecost"
    OBJECT_NAME = "shopee_category_test"
    COUNTRY = "VN"
    LAYER = "Bronze"
    STAGE = "Landed"
    NAME = f"{COUNTRY} Data beecost"
    DESCRIPTION = "test description lorem ipsum"
    DATA_OWNER = "email@example.com"
    PROCESS_OWNER = "email1@example.com"
    IS_ENABLED = True
    CRON = "0 8 * * */1"
    LAYER = "Bronze"
    STAGE = "Landed"
    BROKER = ContractBroker(
        layer=LAYER,
        stage=STAGE,
        data_source=DATA_SOURCE,
        object_name=OBJECT_NAME,
        override_path=TEST_JSON_PATH,
    )
    CONTRACT = BROKER.check_deployment_folder()

    def test_init_data_source(self):
        assert self.CONTRACT.contract_data_source == "beecost"

    def test_init_object_name(self):
        assert self.CONTRACT.contract_object_name == "shopee_category_test"

    def test_init_class_instance(self):
        assert isinstance(self.CONTRACT, UContract)
