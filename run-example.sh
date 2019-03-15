#!/usr/bin/env bash

set -eu
set -o pipefail

REPO_ROOT=$(cd $(dirname $0); pwd)
REPO_NAME=${REPO_ROOT##*/}
EXEC_DAG=${1:-example-dag}
EXEC_TASK=${2:-example-task}
EXEC_DATE=${3:-$(TZ=UTC date '+%F')}
PRESTO_HOST=${PRESTO_HOST:-}
PRESTO_PORT=${PRESTO_PORT:-}
PRESTO_CATALOG=${PRESTO_CATALOG:-}

(
    cd $REPO_ROOT
    mkdir -p logs
    docker build -q -t $REPO_NAME/airflow .
    docker run -it --rm \
               -e AIRFLOW_CONN_PRESTO_DEFAULT=presto://${PRESTO_HOST}:${PRESTO_PORT}/${PRESTO_CATALOG} \
               -v $HOME/.aws:/root/.aws \
               -v $REPO_ROOT/src/airflow/plugin:/root/plugins \
               -v $REPO_ROOT/example:/root/dags \
               -v $REPO_ROOT/logs:/root/logs \
               $REPO_NAME/airflow \
               airflow run --local $EXEC_DAG $EXEC_TASK $EXEC_DATE
    cat logs/${EXEC_DAG}/${EXEC_TASK}/${EXEC_DATE}T00:00:00+00:00/1.log
)

