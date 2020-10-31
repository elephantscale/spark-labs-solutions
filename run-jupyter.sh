#!/bin/bash

## if you need to set anaconda path explicitly, do so here
## how ever, if you are using 'custom python environment', 
## leave this line commented out (do not over-ride the default path)

#export PATH=$HOME/anaconda3/bin:$PATH
export SPARK_HOME=$HOME/apps/spark
jupyter lab

## --- older approach ---
# export PYSPARK_PYTHON=$(which python3)
# export PYSPARK_DRIVER_PYTHON=$(which jupyter)
# export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

## pyspark i local mode
# $SPARK_HOME/bin/pyspark

## pyspark connect to cluster
#$SPARK_HOME/bin/pyspark --master spark://localhost:7077  --executor-memory 4g --driver-memory 1g
