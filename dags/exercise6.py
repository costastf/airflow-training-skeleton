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

import json

import airflow
import requests
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.models import DAG
from airflow.utils.decorators import apply_defaults


class LaunchHook(BaseHook):

    def __init__(self, url='https://launchlibrary.net/1.4/', endpoint='launch'):
        super().__init__(source=None)
        self._url = url + endpoint

    def get_records(self, start_date, end_date):
        params = {'startdate': start_date, 'enddate': end_date}
        return requests.get(self._url, params=params).json()


class LaunchLibraryOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id,
                 endoint,
                 params,
                 bucket,
                 result_key,
                 google_cloud_storage_conn_id,
                 *args,
                 **kwargs):
        super(LaunchLibraryOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.endoint = endoint
        self.params = params
        self.bucket = bucket
        self.result_key = result_key
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = None

    def execute(self, context):
        hook = LaunchHook()
        records = hook.get_records(self.params.get('startdate'), self.params.get('enddate'))
        self._upload_to_gcs(records)

    def _upload_to_gcs(self, file):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        hook.upload(self.bucket, json.dumps(file), self.result_key, 'application/json', False)


arguments = {'dag_id': 'exercise6',
             'default_args': {'owner': 'Costas',
                              'start_date': airflow.utils.dates.days_ago(10)},
             'schedule_interval': None}

with DAG(**arguments) as dag:
    launch_library_to_gcp = LaunchLibraryOperator(task_id='launch_library_to_gcp',
                                                  conn_id='launch_library_default',
                                                  endoint='launch',
                                                  params={'startdate': '{{ ds }}', 'enddate': '{{ tomorrow_ds }}'},
                                                  bucket='mydata34534534',
                                                  result_key='launches.json',
                                                  google_cloud_storage_conn_id='google_cloud_default')
