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
PRESTO_CATALOG=${PRESTO_CATALOG:-hive}
WORK_DIR="/root"

(
    cd $REPO_ROOT
    mkdir -p logs
    rm -rf dist/
    poetry build
    pkg_tgz=$(ls dist/*.tar.gz | sort -n | tail -n1)
    pkg=${pkg_tgz%*.tar.gz}
    tar zxvf $pkg_tgz -C dist/
    docker build -q -t $REPO_NAME/airflow .
    docker run -it --rm \
               -e AIRFLOW_CONN_PRESTO_DEFAULT=presto://${PRESTO_HOST}:${PRESTO_PORT}/${PRESTO_CATALOG} \
               -v $HOME/.aws:${WORK_DIR}/.aws \
               -v $REPO_ROOT/dist:${WORK_DIR}/dist \
               -v $REPO_ROOT/example:${WORK_DIR}/dags \
               -v $REPO_ROOT/logs:${WORK_DIR}/logs \
               $REPO_NAME/airflow \
               bash -c "
                   set -x
                   cd $pkg
                   python setup.py install
                   cd $WORK_DIR
                   airflow run --local $EXEC_DAG $EXEC_TASK $EXEC_DATE
               "

    cat logs/${EXEC_DAG}/${EXEC_TASK}/${EXEC_DATE}T00:00:00+00:00/1.log
)

