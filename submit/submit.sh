#!/bin/bash

export SPARK_MASTER_URL=spark://${SPARK_MASTER_NAME}:${SPARK_MASTER_PORT}
export SPARK_HOME=/spark
export SPARK_APPLICATION_PYTHON_LOCATION=/wordcount.py
export PATH_POSTGRES_DRIVER=/postgresql-42.3.5.jar
export PATH_FILE_CSV=/flights_2008_7M.csv
export SPARK_APPLICATION_ARGS=/README.md
/wait-for-step.sh
/execute-step.sh

if [ ! -z "${SPARK_APPLICATION_JAR_LOCATION}" ]; then
    echo "Submit application ${SPARK_APPLICATION_JAR_LOCATION} with main class ${SPARK_APPLICATION_MAIN_CLASS} to Spark master ${SPARK_MASTER_URL}"
    echo "Passing arguments ${SPARK_APPLICATION_ARGS}"
    /${SPARK_HOME}/bin/spark-submit \
        --class ${SPARK_APPLICATION_MAIN_CLASS} \
        --master ${SPARK_MASTER_URL} \
        ${SPARK_SUBMIT_ARGS} \
        ${SPARK_APPLICATION_JAR_LOCATION} ${SPARK_APPLICATION_ARGS}
else
    if [ ! -z "${SPARK_APPLICATION_PYTHON_LOCATION}" ]; then
        echo "Submit application ${SPARK_APPLICATION_PYTHON_LOCATION} to Spark master ${SPARK_MASTER_URL}"
        echo "Passing arguments ${PATH_POSTGRES_DRIVER} and ${PATH_FILE_CSV}"
        PYSPARK_PYTHON=python3  /spark/bin/spark-submit \
            --master ${SPARK_MASTER_URL} \
            --driver-class-path ${PATH_POSTGRES_DRIVER}\
            ${SPARK_APPLICATION_PYTHON_LOCATION} 
        echo "${SPARK_APPLICATION_PYTHON_LOCATION}"
        
    else
        echo "Not recognized application."
        
    fi
fi

/finish-step.sh
