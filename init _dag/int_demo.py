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
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.connection import Connection
from airflow import settings
from airflow.models import Connection

import logging
import shutil
import sys
import tempfile
from datetime import date
from pprint import pprint
import csv
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
from airflow.models.xcom import XCom

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable
IN_FILE = '/opt/airflow/dags/POC_CNS/file_2_mysql/IN/customers.csv'
BASE_DIR = tempfile.gettempdir()
#Mysql
CONN_ID_MYSQL = 'poc_mysql'
CONN_TYPE_MYSQL = 'mysql'
CONN_HOST_MYSQL = '172.18.0.4'
CONN_USR_MYSQL = 'root'
CONN_PSW_MYSQL = 'rjon2457'
CONN_PORT_MYSQL = '3306'

#Postgre
CONN_ID_PSQL = 'poc_psql'
CONN_TYPE_PSQL = 'pssql'
CONN_USR_PSQL = 'airflow'
CONN_HOST_PSQL = '172.18.0.3'
CONN_PSW_PSQL = 'airflow'
CONN_PORT_PSSQL = '5432'

#Mssql
CONN_ID_MSSQL = 'poc_mssql'
CONN_TYPE_MSSQL = 'mssql'
CONN_USR_MSSQL = 'root'
CONN_HOST_MSSQL = 'root'
CONN_PSW_MSSQL = 'rjon2457'

#Oracle
CONN_ID_ORA = 'poc_oracle'
CONN_TYPE_ORA = 'oracle'
CONN_USR_ORA = 'root'
CONN_PSW_ORA = 'rjon2457'
CONN_HOST_ORA = 'root'


#Firebird
CONN_ID_FB = 'poc_firebird'
CONN_TYPE_FB = 'firebird'
CONN_USR_FB = 'root'
CONN_PSW_FB = 'rjon2457'
CONN_HOST_FB = 'root'
with DAG(
    dag_id="init_db_demo",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["init_db_demo"],
) as dag:

    # [START read_data_mysql]
    @task(task_id="create_connections")
    def create_connections(ds=None, **kwargs):
        #Create connection
        conn = Connection(
            conn_id=CONN_ID_MYSQL,
            conn_type=CONN_TYPE_MYSQL,
            host=CONN_HOST_MYSQL,
            login=CONN_USR_MYSQL,
            password=CONN_PSW_MYSQL,
            port=CONN_PORT_MYSQL
        ) #create a connection object
        session = settings.Session() # get the session
        session.add(conn)
        session.commit() # it will insert the connection object programmatically.
    create_connections_tasks = create_connections()
    # [END create_connections]

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
    create_connections_tasks
    #>> [load_data_task_mssql]
