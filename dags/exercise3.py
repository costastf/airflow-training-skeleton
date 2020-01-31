# -*- coding: utf-8 -*-
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

"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

arguments = {'dag_id': 'exercise3',
             'default_args': {'owner': 'Costas',
                              'start_date': airflow.utils.dates.days_ago(2)},
             'schedule_interval': None}


def _print_exec_date(**context):
    print(context["execution_date"])


with DAG(**arguments) as dag:
    task1 = PythonOperator(task_id='print_execution_date',
                           python_callable=_print_exec_date,
                           provide_context=True)
    task2 = BashOperator(task_id='wait_1', bash_command="sleep 1")
    task3 = BashOperator(task_id='wait_5', bash_command="sleep 5")
    task4 = BashOperator(task_id='wait_10', bash_command="sleep 10")
    task5 = DummyOperator(task_id='the_end')

task1 >> [task2, task3, task4] >> task5
