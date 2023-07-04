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
from airflow.hooks.base import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
import logging
import shutil
import sys
import tempfile
#import time
from datetime import date
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
CONN_ID = 'poc_mysql'

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

        #for row in records:
        #    log.info(row)
        #    #apply_some_rules
        kwargs['ti'].xcom_push(key='records', value=records)
    validate_data_task = validate_data()
    #[END validate_data]
    # [START transform data]
    @task(task_id="transform_data")
    def transform_data(ds=None, **kwargs):
        records = kwargs['ti'].xcom_pull(key='records')
        output_records = []
        output_record = []
        first = False
        for row in records:
            if first:
                output_record = []
                output_record.append(row[0])
                output_record.append(row[1])
                output_record.append(row[2])
                output_record.append(row[3])
                output_record.append(row[4])
                output_record.append(row[5])
                output_record.append(row[6])
                output_record.append(row[7])
                output_record.append(row[8])
                output_record.append(row[9])
                output_record.append(row[10])
                output_record.append(row[11])
                output_record.append(row[12])
                output_record.append(date.today())
                output_records.append(output_record)
            else:
                first = True
        kwargs['ti'].xcom_push(key='records', value=output_records)
    transform_data_task =transform_data()
    #[END transform_data]
    # [START load data]
    @task(task_id="load_data")
    def load_data(ds=None, **kwargs):
        records = kwargs['ti'].xcom_pull(key='records')
        output_records = []
        output_record = []
        target_fields = []
        source = MySqlHook(CONN_ID)
        conn = source.get_conn()
        cursor = conn.cursor()

        #target_fields.append('id')
        target_fields.append('first_name')
        target_fields.append('last_name')
        target_fields.append('email')
        target_fields.append('phone')
        target_fields.append('address')
        target_fields.append('gender')
        target_fields.append('age')
        target_fields.append('registered')
        target_fields.append('orders')
        target_fields.append('spent')
        target_fields.append('job')
        target_fields.append('hobbies')
        target_fields.append('is_married')
        target_fields.append('creation_date')
        source.insert_rows('poc_db.customers',
                           records,
                           target_fields=target_fields,
                           replace_index=None,
                           replace=True)
        cursor.close()
        conn.close()
        kwargs['ti'].xcom_push(key='records', value=output_records)

    load_data_task = load_data()
    # [END load_data]


    ingest_file_task >> validate_data_task >> transform_data_task >> load_data_task