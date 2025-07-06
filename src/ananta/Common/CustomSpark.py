import os

from pyspark.sql import DataFrame, SparkSession

__doc__ = """Custom Spark for local dev """


def _setup_local_session():
    """Setup local session as close as ADB 10.4LTS"""
    import pydeequ

    root = os.path.dirname(os.path.realpath(os.getcwd()))
    classpath = f"{root}/jar/"
    spark = (
        SparkSession.builder.appName("Local")
        .master("local[*]")
        .enableHiveSupport()
        .config("spark.driver.extraClassPath", classpath)
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.0.1,com.amazon.deequ:deequ:2.0.1-spark-3.2",
        )
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.execution.arrow.pyspark.enabled", True)
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", True)
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.debug.maxToStringFields", 10000)
        .config("spark.memory.offHeap.enabled", True)
        .config("spark.memory.offHeap.size", "12g")
    )
    return spark


def get_spark_session():
    """Get Spark Session main function"""
    if os.environ.get("LOCAL_DEV", False):
        spark = _setup_local_session()
    else:
        spark = SparkSession.builder
    return spark.getOrCreate()
