import os
import json

__doc__ = """
Generate Messages in application.

This to improve readablity and locale support later on support
"""


# TODO: Country locale message
def get_messages_data():
    with open(f"{os.getcwd()}/src/ananta/Common/locale/EN.json", encoding="utf8") as f:
        return json.load(f)
