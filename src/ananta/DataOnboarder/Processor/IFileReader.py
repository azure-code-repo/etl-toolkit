import pandas as pd

from .FileReader import FileReader

__doc__ = """Implementation for FileReader Base class"""


class CsvFileReader(FileReader):
    """Read CSV/TSV file with builder"""

    def __init__(self, **data):
        super().__init__(**data)

    def prepare(self, **options):
        """Read CSV/TSV file with builder

        Builder using spark.read.format('csv')

        :param file_path: path string
        :type file_path: str
        :param schema: schema as json
        :type schema: json
        :param sep: seperator string ```, | \t ;```
        :type sep: str
        :param options: Override pyspark.build option
        i.e. ```{
            "header": True,
            "multiLine": True,
            "wholeFile": True,
            "escape": '"',
            "sep": options.get("sep", ","),
        }```
        :type options: dict
        :return: pyspark dataframe
        :rtype: dataframe
        """
        default_option = {
            "header": True,
            "multiLine": True,
            "wholeFile": True,
            "escape": '"',
        }
        if self.meta_data["file"]["ext"] == "csv":
            default_option["sep"] = ","
        elif self.meta_data["file"]["ext"] == "tsv":
            default_option["sep"] = "\t"
        ext_options = options.get("options", default_option)
        builder = self.spark.read.format("csv")
        if self.meta_data:
            builder.schema(self.meta_data["schema"]["struct"])
        else:
            builder.option("inferSchema", True)
        for option in ext_options:
            builder.option(option, ext_options[option])
        return builder

    def read(self, **options):
        builder = self.prepare().format("csv")
        if isinstance(self.file_path, list):
            return builder.load(*self.file_path)
        return builder.load(self.file_path)


class ExcelFileReader(FileReader):
    """Read CSV/TSV file with builder"""

    def __init__(self, **data):
        super().__init__(**data)

    def prepare(self, **options):
        """Read Excel file with builder

        Files are read with pandas then convert to pyspark dataframe

        :param file_path: path string
        :type file_path: str
        :param header: header of the excel sheet, defaults to 0
        :type header: int
        :param name: column name override , defaults to none
        :type name: list
        :param skiprows: skiprows of the excel sheet, defaults to 0
        :type skiprows: int
        :param skipfooter: skipfooter of the excel sheet, defaults to 0
        :type skipfooter: int
        :param sheet_name: sheet_name of the excel sheet, defaults to 0
        :type sheet_name: name
        :return: pyspark dataframe
        :rtype: dataframe
        """
        default_option = {
            "header": options.get("header", 0),
            "name": options.get("name", None),
            "engine": "openpyxl",
            "skiprows": options.get("skiprows", 0),
            "nrows": options.get("nrows", 0),
            "skipfooter": options.get("skipfooter", 0),
            "sheet_name": options.get("sheet_name", None),
        }
        if default_option["nrows"] == 0:
            del default_option["nrows"]
        if not default_option["sheet_name"]:
            del default_option["sheet_name"]

    def read(self, **options):
        if isinstance(self.file_path, list):
            pd_df = pd.concat(pd.read_excel(excelFile) for excelFile in self.file_path)
        else:
            pd_df = pd.read(self.file_path)
        return self.spark.createDataFrame(pd_df)


class ParquetFileReader(FileReader):
    """Read CSV/TSV file with builder"""

    def __init__(self, **data):
        super().__init__(**data)

    def prepare(self, **options):
        pass

    def read(self, **options):
        """Read Excel file with builder

        Files are read with pandas then convert to pyspark dataframe

        :param file_path: path string
        :type file_path: str
        :return: pyspark dataframe
        :rtype: dataframe
        """

        if isinstance(self.file_path, list):
            return self.spark.read.format("parquet").parquet(*self.file_path)
        return self.spark.read.format("parquet").parquet(self.file_path)
