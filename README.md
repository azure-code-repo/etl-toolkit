# Introduction
TODO: Give a short introduction of your project. Let this section explain the objectives or the motivation behind this project.

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Build and Test
TODO: Describe and show how to build your code and run the tests.
## Documentation
sphinx-apidoc -f -o docs src/ananta -l -d 5 -M
make html
- Output Docs
- Use docs/index.html
# Contribute
- PtPashantTripathi@outlook.com
- PtPashantTripathi@outlook.com

### VScode Tasks
```json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "build wheel",
            "type": "shell",
            "command": "python -m build"
        },
        {
            "label": "build docs",
            "type": "shell",
            "command": "sphinx-apidoc -f -o docs src/ananta -l -d 5 -M"
        }
    ]
}
```

# Local installation for development

## Install Java
version = jre1.8.0_341

## Install Spark
version = spark-3.2.1-bin-hadoop3.2-scala2.13.tgz - [Download link](https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2-scala2.13.tgz)

## Install python
version = 3.8.13

## Get Winutils for Hadoop on windows
https://github.com/cdarlint/winutils

## Setup PATH local / system on windows
Setup following User Variables under Windows> Path as follow:
- HADOOP_HOME
    - winutils location
    - 3.2.1
- LOCAL_DEV
    - TRUE
- PYSPARK_PYTHON
    - python location
    - If you are using conda/env use its python path
- PYTHONPATH
    - python location
    - If you are using conda/env use its python path
- SPARK_HOME
    - location of spark downloaded and extracted
- SPARK_LOCAL_DIRS (_optional_)
- SPARK_VERSION
    - 3.2.1

Setup environments variable as follow:
- %SPARK_HOME%\bin
- %SPARK_HOME%\python
- %SPARK_HOME%\python\lib\py4j-0.10.9.5-src.zip
- %HADOOP_HOME%\bin

### Setup local Spark with Delta Live table
pyspark --packages io.delta:delta-core_2.12:1.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

### Fix Bugs
#### Pyspark delta core to scala 2.13
- configure_spark_with_delta_pip(config).getOrCreate()
- Go to configure_spark_with_delta_pip which is pip_utils.py
- scroll down to row 77
- change 12 => 13
### Ananta



Ananta - Data Onboarder 0.0.1 documentation












[Ananta - Data Onboarder](#)





[Ananta - Data Onboarder](#)


* Ananta - Data Onboarder 0.0.1 documentation

---



# Welcome to Ananta’s documentation![](#welcome-to-ananta-s-documentation "Link to this heading")



# Indices and tables[](#indices-and-tables "Link to this heading")

* [Index](genindex.html)
* [Module Index](py-modindex.html)
* [Search Page](search.html)



---


© Copyright 2022,.


Built with [Sphinx](https://www.sphinx-doc.org/) using a
[theme](https://github.com/readthedocs/sphinx_rtd_theme)
provided by [Read the Docs](https://readthedocs.org).
# etl-toolkit
