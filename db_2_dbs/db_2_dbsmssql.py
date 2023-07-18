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
#from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.connection import Connection

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
CONN_ID_PSQL = 'poc_psql'
CONN_ID_MSSQL = 'poc_mssql'

with DAG(
    dag_id="db_2_dbsmssqls",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["file_2_mssql"],
) as dag:

    # [START read_data_mysql]
    @task(task_id="read_data_mysql")
    def read_data(ds=None, **kwargs):
        records_readed = []
        source = MySqlHook(CONN_ID)
        conn = source.get_conn()
        cursor = conn.cursor()
        records_readed = source.get_records('SELECT * from customers')
        cursor.close()
        conn.close()
        kwargs['ti'].xcom_push(key='records', value=records_readed)

    read_data_task = read_data()
    # [END read_data_mysql]

#    # [START insert_mssql_hook]
#    @dag.task(task_id="insert_mssql_task")
#    def insert_mssql_hook(**kwargs):
#        mssql_hook = MsSqlHook(mssql_conn_id="poc_mssql", schema="poc_db")
#        records = kwargs['ti'].xcom_pull(key='records')
#        target_fields = []
#        target_fields.append('id')
#        target_fields.append('first_name')
#        target_fields.append('last_name')
#        target_fields.append('email')
#        target_fields.append('phone')
#        target_fields.append('address')
#        target_fields.append('gender')
#        target_fields.append('age')
#        target_fields.append('registered')
#        target_fields.append('orders')
#        target_fields.append('spent')
#        target_fields.append('job')
#        target_fields.append('hobbies')
#        target_fields.append('is_married')
#        target_fields.append('creation_date')
#        mssql_hook.insert_rows(table="customers", rows=records, target_fields=target_fields)

 #   load_data_task_mssql = insert_mssql_hook()
    # [END insert_mssql_hook]
    read_data_task
    #>> [load_data_task_mssql]