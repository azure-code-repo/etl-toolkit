import json

import pandas as pd
from pydantic import BaseModel
from ananta.DataContract.Contract import UContract, UContractField


class CreateContract(BaseModel):
    def from_excel_form(self, path):
        excel_file = pd.ExcelFile(path)
        change_log_df = pd.read_excel(
            excel_file, sheet_name="Change Log", header=1, usecols="A:E"
        )
        cols_clean_up = [
            "Requestor Detail & Owner Details",
            "Source Details",
            "Ingestion design pattern",
        ]
        cols_clean_up = [
            col.replace(" ", "_").replace(",", "").lower() for col in cols_clean_up
        ]
        meta_data_df_raw = pd.read_excel(
            excel_file, sheet_name="Details & Approval", header=5, usecols="B:C"
        )
        meta_data_df = meta_data_df_raw.transpose().reset_index()
        header = meta_data_df.iloc[0]

        meta_data_df.columns = [
            h.replace(" ", "_").replace(",", "").lower() for h in header
        ]
        meta_data_df.drop(index=0, axis=0, inplace=True)
        meta_data_df.drop(labels=cols_clean_up, axis=1, inplace=True)
        meta_data = meta_data_df.to_dict(orient="records")
        fields_df = pd.read_excel(excel_file, sheet_name="DMR", header=1, usecols="A:N")
        fields_df.columns = [
            h.replace(" ", "_").replace(",", "").lower() for h in fields_df.columns
        ]
        # print(fields_df.info())

        select_cols = [
            "source_attribute",
            "source_description",
            "source_datatype",
            "source_primary_key",
            "logic",
            "active_flag",
        ]
        contract_fields = []
        for field in fields_df[select_cols].iterrows():
            item = UContractField(
                field_name=field[1]["source_attribute"].lower(),
                field_type=UContractField.get_correct_type(
                    field[1]["source_attribute"], field[1]["source_datatype"]
                ),
                field_is_primary=(
                    True if field[1]["source_primary_key"] == "Y" else False
                ),
                field_description=field[1]["source_description"],
            )
            contract_fields.append(item)
        # print(contract_fields)

        contry_factory = {
            "SEAID": "SEAID",
            "vietnam": "VN",
            "indonesia": "ID",
            "thailand": "TH",
            "phillipine": "PH",
            "malaysia": "MY",
            "cambodia": "KH",
            "laos": "LA",
            "singapore": "SG",
        }

        contract = UContract(
            contract_country=contry_factory.get(meta_data[0]["scope"].lower(), "SEAID"),
            contract_description=meta_data[0]["description"],
            contract_name=f'{contry_factory.get(meta_data[0]["scope"].lower(), "SEAID")} - {meta_data[0]["department"]} - {meta_data[0]["layer"]} - Landed - {meta_data[0]["source"]} - {meta_data[0]["data_name"]}',
            contract_layer=meta_data[0]["layer"],
            contract_stage="Landed",
            contract_object_name=meta_data[0]["data_name"],
            contract_data_source=meta_data[0]["source"],
            contract_data_owner=meta_data[0]["owner_email"],
            contract_process_owner=meta_data[0]["owner_email"],
            contract_fields=contract_fields,
        )
        return contract

    def to_json(self, contract):
        return json.dumps(contract, indent=4)

    def promote_landed_contract_to_processed(self, contract):
        contract.contract_name = contract.contract_name.replace(
            "- Landed -", "- Processed -"
        )
        contract.contract_stage = "Processed"
        return contract
