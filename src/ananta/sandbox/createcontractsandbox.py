import os
import sys
import json
import shutil
from sys import platform

print(platform)
if os.environ.get("LOCAL_DEV", False):
    if platform == "linux" or platform == "linux2":
        # linux
        pass
    elif platform == "darwin":
        print(f"path added")
        sys.path.insert(0, "/Users/kom/code/ananta/src")

    elif platform == "win32":
        sys.path.insert(0, "C:/Users/PtPashantTripathi/code/ananta/src")

from ananta.utilities import CreateContract

shutil.rmtree("./data/DataContract", ignore_errors=True)


def write_to_json(contract):
    path = (
        os.getcwd().replace("\\", "/")
        + f"/src/ananta/sandbox/data/DataContract/{contract.contract_layer}/{contract.contract_stage}/{contract.contract_country}/"
    )
    os.makedirs(
        path,
        exist_ok=True,
    )
    with open(
        f"{path}/{contract.contract_data_source}_{contract.contract_object_name}.json",
        "w+",
    ) as f:
        f.write(contract.json())


def generate(path):
    contract = CreateContract().from_excel_form(f"./forms/{path}")
    write_to_json(contract=contract)
    processed_contract = CreateContract().promote_landed_contract_to_processed(contract)
    print(processed_contract)
    write_to_json(contract=processed_contract)


for path in list(
    filter(None, [x if ".xlsx" in x else None for x in os.listdir("./forms/")])
):
    print(f"---{path}")
    generate(path)
shutil.copytree("./forms/DataContract", "./data/DataContract")
shutil.rmtree("./forms/DataContract", ignore_errors=True)
