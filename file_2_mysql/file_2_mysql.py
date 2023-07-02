#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""
from __future__ import annotations

import logging
import shutil
import sys
import tempfile
#import time
from datetime import datetime
from pprint import pprint
import csv
import pendulum
#from colorama import Back, Fore, Style
#from airflow.models.xcom import XCom
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
from airflow.models.xcom import XCom
log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable
IN_FILE = '/opt/airflow/dags/POC_CNS/file_2_mysql/IN/customers.csv'
BASE_DIR = tempfile.gettempdir()


def x():
    pass


with DAG(
    dag_id="file_2_mysql",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["file_2_mysql"],
) as dag:

    # [START ingest file]
    @task(task_id="ingest_file")
    def ingest_file(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        log.warning( "File ingestion " + ' customers.csv ')
        results = []

        with open(IN_FILE, 'r') as file:
          csvreader = csv.reader(file)
          for row in csvreader:
            results.append(row)
        kwargs['ti'].xcom_push(key='records', value=results)
    ingest_file_task = ingest_file()
    #[END _ingest file]

    # [START validate_data]
    @task(task_id="validate_data")
    def validate_data(ds=None, **kwargs):
        records = kwargs['ti'].xcom_pull(key='records')
        for row in records:
            log.info(row)
            #apply_some_rules
    validate_data_task = validate_data()
    #[END validate_data]

    # [START transform data]
    @task(task_id="transform_data")
    def transform_data(ds=None, **kwargs):
        records = kwargs['ti'].xcom_pull(key='records')
        for row in records:
            row[13] = datetime.today().strftime('%Y-%m-%d')
        kwargs['ti'].xcom_push(key='records', value=records)
    transform_data_task =transform_data()
    #[END transform_data]

    ingest_file_task >> validate_data_task >> transform_data_task
    #>> load_data



