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

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

arguments = {'dag_id': 'exercise4',
             'default_args': {'owner': 'Costas',
                              'start_date': airflow.utils.dates.days_ago(2)},
             'schedule_interval': None}


def get_person_for_day(**context):
    person_map = {'Mon': 'Bob',
                  'Tue': 'Joe',
                  'Wed': 'Alice',
                  'Thu': 'Joe',
                  'Fri': 'Alice',
                  'Sat': 'Alice',
                  'Sun': 'Alice'}
    return f'email_{person_map.get(context["execution_date"].strftime("%a")).lower()}'


with DAG(**arguments) as dag:
    print_weekday = PythonOperator(task_id='print_weekday',
                                   python_callable=lambda **context: print(
                                       (f'I am printing the execution date from a lambda'
                                        f':{context["execution_date"].strftime("%a")}')),
                                   provide_context=True)
    branching = BranchPythonOperator(task_id='branching',
                                     python_callable=get_person_for_day,
                                     provide_context=True)
    email_bob = DummyOperator(task_id='email_bob')
    email_alice = DummyOperator(task_id='email_alice')
    email_joe = DummyOperator(task_id='email_joe')
    final_task = BashOperator(task_id='final_task', bash_command='echo DONE!', trigger_rule='one_success')


print_weekday >> branching
branching >> email_bob >> final_task
branching >> email_alice >> final_task
branching >> email_joe >> final_task
