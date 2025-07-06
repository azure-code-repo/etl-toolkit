#!/bin/bash
# PIP CONFIG
PIP_CONF=/etc/pip.conf
if [ ! -f "$PIP_CONF" ]; then
	echo "$PIP_CONF not found! Generating $PIP_CONF"
	echo """[global]
index-url=https://${AZ_ANANTA_FEED_NAME}:${AZ_ANANTA_TOKEN}@pkgs.dev.azure.com/${AZ_ANANTA_ORG_NAME}/${AZ_ANANTA_PROJECT_NAME}/_packaging/${AZ_ANANTA_FEED_NAME}/pypi/simple/
""" > $PIP_CONF
else
	echo "$PIP_CONF found! Skip"
fi
# JAR FILE
JAR_FILE=/dbfs/databricks/jars/deequ-2.0.1-spark-3.2.jar
if [ ! -f "$JAR_FILE" ]; then
    echo "$JAR_FILE not found!"
    pydeequ_jar_url="https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.1-spark-3.2/deequ-2.0.1-spark-3.2.jar"
    wget -N -q $pydeequ_jar_url -O $JAR_FILE
    mkdir -p /dbfs/databricks/jars
    cp /dbfs/mnt/databricks/jars/ananta/deequ-2.0.1-spark-3.2.jar $JAR_FILE
	echo " - Download and copy to $JAR_FILE completed!"
else
	echo "$JAR_FILE found! Skip"
fi
pip install keyring artifacts-keyring pydantic pydeequ pyarrow
pip uninstall ananta -y
pip install ananta
