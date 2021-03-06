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

from tempfile import NamedTemporaryFile

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator
)
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.decorators import apply_defaults

arguments = {'dag_id': 'exercise7',
             'default_args': {'owner': 'Costas',
                              'start_date': '2019-11-27'},
             'schedule_interval': None}


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action
    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """
    template_fields = ("endpoint", "gcs_path")
    template_ext = ()
    ui_color = "#F4A460"

    @apply_defaults
    def __init__(self,
                 endpoint,
                 gcs_bucket,
                 gcs_path,
                 method="GET",
                 http_conn_id="http_default",
                 gcs_conn_id="google_cloud_default",
                 *args,
                 **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.gcs_conn_id = gcs_conn_id

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        self.log.info("Calling HTTP method")
        response = http.run(self.endpoint)
        with NamedTemporaryFile() as tmp_file_handle:
            tmp_file_handle.write(response.content)
            tmp_file_handle.flush()
            hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcs_conn_id)
            hook.upload(
                bucket=self.gcs_bucket,
                object=self.gcs_path,
                filename=tmp_file_handle.name,
            )


with DAG(**arguments) as dag:
    postgres_to_gcs = PostgresToGoogleCloudStorageOperator(task_id='postgres_to_gcs',
                                                           sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '2019-11-27'",
                                                           bucket='output_bucket_for_airflow',
                                                           filename='prices-{{ ds }}.json',
                                                           postgres_conn_id='postgres_default',
                                                           google_cloud_storage_conn_id='google_cloud_storage_default')
    http_to_gcs = HttpToGcsOperator(task_id='http_to_gcs',
                                    endpoint='history?start_at={{ yesterday_ds }}&end_at={{ ds }}&symbols=EUR&base=GBP',
                                    gcs_bucket='output_bucket_for_airflow',
                                    gcs_path='exchange-rates-{{ ds }}.json',
                                    method="GET",
                                    http_conn_id="http_default",
                                    gcs_conn_id="google_cloud_default")
    create_cluster = DataprocClusterCreateOperator(task_id='create_cluster',
                                                   project_id='afspfeb3-28e3a1b32a56613ef127e',
                                                   cluster_name='analyse-pricing-{{ ds }}',
                                                   num_workers=2,
                                                   zone='europe-west4-a')
    calculate_statistics = DataProcPySparkOperator(task_id='calculate_statistics',
                                                   main='gs://output_bucket_for_airflow/build_statistics.py',
                                                   arguments=['gs://output_bucket_for_airflow/prices-{{ ds }}.json',
                                                              'gs://output_bucket_for_airflow/exchange-rates-{{ ds }}.json',
                                                              'gs://output_bucket_for_airflow/output.parquet',
                                                              'EUR',
                                                              '{{ yesterday_ds }}'],
                                                   cluster_name='analyse-pricing-{{ ds }}')
    delete_cluster = DataprocClusterDeleteOperator(task_id='delete_cluster',
                                                   cluster_name='analyse-pricing-{{ds}}',
                                                   project_id='afspfeb3-28e3a1b32a56613ef127e',
                                                   region='global')
    # statistics_to_big_query = GoogleCloudStorageToBigQueryOperator(task_id='statistics_to_big_query',
    #                                                                bucket='output_bucket_for_airflow',
    #                                                                source_objects=[''],
    #                                                                destination_project_dataset_table,
    #                                                                schema_fields=None,
    #                                                                schema_object=None,
    #                                                                source_format='CSV',
    #                                                                bigquery_conn_id='bigquery_default',
    #                                                                google_cloud_storage_conn_id='google_cloud_default')

[postgres_to_gcs, http_to_gcs] >> create_cluster >> calculate_statistics >> delete_cluster # >> statistics_to_big_query
