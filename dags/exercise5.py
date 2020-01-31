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

import datetime

from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.models import DAG

arguments = {'dag_id': 'exercise5',
             'default_args': {'owner': 'Costas',
                              'start_date': datetime.datetime.today()},
             'schedule_interval': None}

with DAG(**arguments) as dag:
    postgres_to_gcs = PostgresToGoogleCloudStorageOperator(task_id='postgres_to_gcs',
                                                           sql='SELECT * FROM land_registry_price_paid_uk LIMIT 10',
                                                           bucket='output_bucket_for_airflow',
                                                           filename='output_file',
                                                           postgres_conn_id='postgres_default',
                                                           google_cloud_storage_conn_id='google_cloud_storage_default ')
