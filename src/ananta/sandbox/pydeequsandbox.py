import os
import sys

root = os.path.dirname(os.path.realpath(os.getcwd()))
deequ_jar = "https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.1-spark-3.2/deequ-2.0.1-spark-3.2.jar"
classpath = f"{root}/jar/deequ-2.0.1-spark-3.2.jar"

print(root)
print(classpath)

import pydeequ
from pyspark.sql import Row, SparkSession

spark = (
    SparkSession.builder.config("spark.driver.extraClassPath", classpath)
    .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.1-spark-3.2")
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate()
)

df = spark.sparkContext.parallelize(
    [Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="baz", b=3, c=None)]
).toDF()

from pydeequ.profiles import *

result = ColumnProfilerRunner(spark).onData(df).run()

for col, profile in result.profiles.items():
    print(profile)
