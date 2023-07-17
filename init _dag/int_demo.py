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
CONN_PSW_MYSQL = 'helloworld'
CONN_PORT_MYSQL = '3306'

#Postgre
CONN_ID_PSQL = 'poc_psql'
CONN_TYPE_PSQL = 'pssql'
CONN_USR_PSQL = 'airflow'
CONN_HOST_PSQL = '172.18.0.3'
CONN_PSW_PSQL = 'airflow'
CONN_PORT_PSQL = '5432'

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
        try:
            #Create connection MySQL
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
        except:
            log.warning('Error creando conexión MySQL')
        try:
            #Create connection MySQL
            conn = Connection(
                conn_id=CONN_ID_PSQL,
                conn_type=CONN_TYPE_PSQL,
                host=CONN_HOST_PSQL,
                login=CONN_USR_PSQL,
                password=CONN_PSW_PSQL,
                port=CONN_PORT_PSQL
            ) #create a connection object
            session = settings.Session() # get the session
            session.add(conn)
            session.commit() # it will insert the connection object programmatically.
        except:
            log.warning('Error creando conexión Postgre')

        #TODO: Create connections Oracle
        #TODO: Create connections SQL Server
        #TODO: Create connections SQL Firebird


    create_connections_task = create_connections()
    # [END create_connections]

    @task(task_id="load_data")
    def create_schemas(ds=None, **kwargs):
        # Create MySQL Schemas
        source = MySqlHook(CONN_ID_MYSQL)
        conn = source.get_conn()
        cursor = conn.cursor()
        try:
            source.run( 'drop table if exists POC_CNS.customers')
            source.run( 'drop database if exists POC_CNS')
            source.run( 'CREATE DATABASE POC_CNS')
            source.run(" create table POC_CNS.customers ("
                        "    id            int auto_increment primary key,"
                        "    first_name    varchar(150) not null,"
                        "    last_name     varchar(150) not null,"
                        "    email         varchar(45)  not null,"
                        "    phone         varchar(45)  not null,"
                        "    address       varchar(150) not null,"
                        "    gender        varchar(45)  not null,"
                        "    age           int          not null,"
                        "    registered    datetime     null,"
                        "    orders        int          null,"
                        "    spent         decimal      null,"
                        "    job           varchar(45)  null,"
                        "    hobbies       varchar(150) null,"
                        "    is_married    tinyint      null,"
                        "    creation_date datetime     null"
                        ")"
                       )
        except:
            log.error('Was an error in creation schemas')
        cursor.close()
        conn.close()

        # Create PSQL Schemas
        source = PostgresHook(CONN_ID_PSQL)
        conn = source.get_conn()
        cursor = conn.cursor()
        try:
            source.run( 'drop table if exists POC_CNS.customers')
            source.run( 'drop schema if exists POC_CNS')
            source.run( 'CREATE schema POC_CNS')
            source.run(" create table POC_CNS.customers ("
                        "    id            integer      primary key,"
                        "    first_name    varchar,"
                        "    last_name     varchar,"
                        "    email         varchar,"
                        "    phone         varchar,"
                        "    address       varchar,"
                        "    gender        varchar,"
                        "    age           integer,"
                        "    registered    date,"
                        "    orders        integer,"
                        "    spent         numeric,"
                        "    job           varchar,"
                        "    hobbies       varchar,"
                        "    is_married    integer,"
                        "    creation_date date"
                        ")"
                       )
        except:
            log.error('Was an error in creation schemas')
        cursor.close()
        conn.close()


    create_schemas_task = create_schemas()

    create_connections_task >> create_schemas_task
