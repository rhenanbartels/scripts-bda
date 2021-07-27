#!/bin/sh
export PYTHONIOENCODING=utf8

spark-submit --master yarn --deploy-mode client \
    --keytab "/home/mpmapas/keytab/mpmapas.keytab" \
    --principal mpmapas \
    --queue root.mpmapas \
    --num-executors 1 \
    --driver-memory 10g \
    --executor-cores 1 \
    --executor-memory 10g \
    --conf spark.debug.maxToStringFields=2000 \
    --conf spark.executor.memoryOverhead=2g \
    --conf spark.network.timeout=900 \
    --conf spark.speculation=true \
    --conf spark.locality.wait=0 \
    --conf spark.shuffle.io.numConnectionsPerPeer=2 \
    --conf spark.shuffle.io.maxRetries=5 \
    --conf spark.shuffle.io.retryWait=15s \
    --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE="/tmp" \
    --conf spark.yarn.appMasterEnv.PYTHON_EGG_DIR="/tmp" \
    --conf spark.executorEnv.PYTHON_EGG_DIR="/tmp" \
    --conf spark.executorEnv.PYTHON_EGG_CACHE="/tmp" \
    --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
    --py-files ../utilities/*.py,src/*.py,packages/*.egg,packages/*.whl,packages/*.zip\
    src/detran_ftp_veiculo.py $@ > log.txt
