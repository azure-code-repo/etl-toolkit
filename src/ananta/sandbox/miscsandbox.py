import os
import sys

if os.environ.get("LOCAL_DEV", False):
    sys.path.insert(0, "C:/Users/PtPashantTripathi/code/ananta/src")
from ananta.core import Ananta
from ananta.Common import Messages, Functions, CustomLogger
from ananta.DataOnboarder import IBaseProcessor
from ananta.DataContract.Contract import UContractField

test = filter(
    lambda x: x.field_is_primary,
    [
        UContractField(
            field_name="ITEM_CODE",
            field_type="string",
            field_nullable=True,
            field_is_primary=True,
            field_validations=[{"nullable": False}],
        ),
        UContractField(
            field_name="ITEM_NAME",
            field_type="string",
            field_nullable=True,
            field_is_primary=False,
            field_validations=[],
        ),
        UContractField(
            field_name="BASIC_UOM",
            field_type="string",
            field_nullable=True,
            field_is_primary=True,
            field_validations=[{"values": ["TONS", "CASES"]}],
        ),
        UContractField(
            field_name="WEIGHT",
            field_type="Integer",
            field_nullable=True,
            field_is_primary=False,
            field_validations=[{"values": ">0"}],
        ),
    ],
)
print(list(test))
