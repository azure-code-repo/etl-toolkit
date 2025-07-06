from typing import Any, Optional

from pydantic import Extra, BaseModel
from ananta.Common import CustomSpark

__doc__ = """Custom logger to replace traditional Log"""


class Logger(BaseModel):
    level: Optional[str] = "warn"
    name: Optional[str] = "ANANTA"
    logger: Optional[Any] = None

    class Config:
        extra = Extra.allow
        allow_mutation = True

    def __init__(self, **data):
        super().__init__(**data)
        self.logger = self.get_logger_session()

    def get_logger_session(self):
        spark = CustomSpark.get_spark_session()
        if not spark:
            raise ValueError("Spark is not init")
        logger_manager = spark._jvm.org.apache.log4j.LogManager
        self.logger = logger_manager.getLogger(self.name)
        return self.logger

    def log(self, msg: str = "No message define") -> None:
        """
        Logging message as log4j and print out console
        :param msg: Message to Log, defaults to None
        :type msg: str, optional
        """
        self.logger.warn(msg)  # type: ignore
        print("--{:13}".format(msg))
