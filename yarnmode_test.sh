#!/bin/bash

$SPARK_HOME/bin/spark-submit --master yarn \
    --driver-memory 2G \
    --driver-cores 2 \
    --executor-memory 8G \
    --executor-cores 2 \
    --num-executors 8 \
    /home/erichuang/demo719/cluster_test_mod.py $1
