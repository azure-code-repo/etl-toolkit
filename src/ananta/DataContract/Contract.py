from uuid import UUID, uuid4
from typing import List, Optional
from datetime import datetime

from pydantic import Extra, Field, BaseModel, validator
from ananta.Common import Messages, Constants, Functions
from pyspark.sql.types import (LongType, FloatType, DoubleType, StringType, StructType,
                               BooleanType, IntegerType, StructField, TimestampType)

__doc__ = "Contract Module to integrate with Ananta"


class UContractField(BaseModel):
    """Contract Field"""

    field_name: str
    field_type: str
    field_nullable: Optional[bool] = True
    field_is_primary: Optional[bool] = False
    field_validations: Optional[list] = []
    field_description: Optional[str]

    class Config:
        allow_mutation = True
        extra = Extra.allow

    @validator("field_validations")
    def validator_field_validation(cls, value):
        """Validation for cfield_is_primary must not be null"""
        if value:
            return value
        return None

    @staticmethod
    def get_correct_type(field_name, field_type):
        date_fields = ["created", "datetime", "timestamp", "date"]
        for date_field in date_fields:
            if date_field in field_name.lower():
                return "Timestamp"
        factory = {
            "int": "Integer",
            "bool": "Boolean",
            "long": "Long",
            "double": "Double",
            "timestamp": "Timestamp",
            "str": "String",
            "nvarchar": "String",
            "float": "Float",
        }
        return factory.get(field_type.lower(), "String")

    def _field_type_factory(self) -> StructField:
        """Return StructField based on field type

        :param field_type: String value of field type, Case not senstive
        ```"Timestamp","String","Integer","Long","Boolean","Double"```
        :type field_type: str
        :return: Struct Field
        :rtype: StructField
        """
        file_type_convert = str(self.field_type).lower().title()
        factory = {
            "Timestamp": TimestampType,
            "String": StringType,
            "Integer": IntegerType,
            "Long": LongType,
            "Boolean": BooleanType,
            "Double": DoubleType,
            "Float": FloatType,
        }
        return factory[file_type_convert]()


class UContract(BaseModel):
    """Contract Model to generate to nice json template

    Using Pydantic for validation and control on generation

    :raises ValueError: _description_
    :raises ValueError: _description_
    :return: _description_
    :rtype: _type_
    """

    contract_uuid: UUID = Field(default_factory=uuid4)
    contract_name: str
    contract_description: str
    contract_layer: str
    contract_stage: str
    contract_data_source: str
    contract_object_name: str
    contract_fields: List[UContractField] = []
    contract_data_owner: str
    contract_process_owner: str
    contract_country: str = "SEAID"
    contract_is_enable: Optional[bool] = True
    contract_created_timestamp: Optional[str] = datetime.now().isoformat()
    contract_updated_timestamp: Optional[str] = datetime.now().isoformat()
    contract_cron: str = "0 8 * * */1"  # Default 8:00 everyday

    class Config:
        allow_mutation = True
        extra = Extra.allow

    @validator("contract_country")
    def validator_contract_country(cls, value):
        """Validation for contract_country must not be null"""
        return value.upper()

    @validator("contract_layer")
    def validator_contract_layer(cls, value):
        """Validation for contract_country must not be null"""
        return value.title()

    @validator("contract_stage")
    def validator_contract_stage(cls, value):
        """Validation for contract_country must not be null"""
        return value.title()

    @validator("contract_data_owner", "contract_process_owner")
    def validator_contract_email_data_and_process_owner(cls, value):
        """Validation for contract_data_owner must not be null"""
        if not Functions.ContractFunction.validate_email(value):
            raise ValueError(
                "CONTRACT: VALIDATION: contract_data_owner and contract_process_owner must be and email!"
            )
        return value.lower()

    def get_name(self):
        return f"{self.contract_country}_{self.contract_layer}_{self.contract_stage}_{self.contract_name}_{Functions.convert_string_to_date(self.contract_created_timestamp,fmt='%Y%m%d')}"

    def generate_struct_schema(self):
        schema_builder = []
        for field in self.contract_fields:
            if not isinstance(field, UContractField):
                item = UContractField(**field)
            else:
                item = field
            item_type = item._field_type_factory()
            #         field_name: str
            # field_type: str
            # field_nullable: Optional[bool] = True
            # field_is_primary: Optional[bool] = False
            # field_validations: Optional[list] = []
            # field_description: Optional[str]
            schema_item = StructField(
                f'"{str(item.field_name).lower()}"',
                item_type,
                bool(item.field_nullable),
            )
            schema_builder.append(schema_item)
        return StructType(schema_builder)

    @staticmethod
    def _get_correct_type(field_name, field_type):
        date_fields = ["created", "datetime", "timestamp"]
        for date_field in date_fields:
            if date_field in field_name.lower():
                return "Timestamp"
        factory = {
            "int": "Integer",
            "bool": "Boolean",
            "long": "Long",
            "double": "Double",
            "timestamp": "Timestamp",
            "str": "String",
        }
        return factory.get(field_type.lower(), "String")

    def generate_hive_schema(self):
        result = []
        for i in self.__fields__.values():
            result.append(
                f"{i.name} {UContract._get_correct_type(i.name, i.type_.__name__)}"
            )
        return result

    def generate_json_fields_from_structtype(self): ...

    def generate_primary_keys(self):
        # print(self.contract_fields)
        primary_keys_list = list(
            filter(lambda x: x.field_is_primary is True, self.contract_fields)
        )
        if len(primary_keys_list) == 0:
            raise ValueError("No Primary keys detected")
        return [x.field_name.lower() for x in primary_keys_list]
