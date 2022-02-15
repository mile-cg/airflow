import logging
import os
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator

GCP_CONNECTION_ID = "GCP"
GCP_AIRFLOW_BUCKET = Variable.get("GCP_AIRFLOW_BUCKET")
GCS_BUCKET = Variable.get("GCP_DATAFLOW_BUCKET")
GCP_NETWORK = Variable.get("GCP_DATAFLOW_NETWORK")
GCP_SUBNETWORK = Variable.get("GCP_DATAFLOW_SUBNETWORK")
GCP_LOCATION = Variable.get("GCP_DATAFLOW_LOCATION")
GCP_PROJECT_ID = Variable.get("GCP_DATAFLOW_PROJECT_ID")
JOB_ENV = Variable.get("GCP_DATAFLOW_ENV").lower()
NOW = datetime.today().strftime("%Y-%m-%d")
ARCHIVE_OUT_FILE = "colorado" + NOW

logging.info("ARCHIVE_OUT_FILE: " + ARCHIVE_OUT_FILE)
logging.info("GCP_PROJECT_ID: " + GCP_PROJECT_ID)
logging.info("GCP_PROJECT_ID: " + GCP_PROJECT_ID)
logging.info("GCP_AIRFLOW_BUCKET: " + GCP_AIRFLOW_BUCKET)
logging.info("GCS_BUCKET: " + GCS_BUCKET)
logging.info("GCP_SUBNETWORK: " + GCP_NETWORK)
logging.info("GCP_SUBNETWORK: " + GCP_SUBNETWORK)
logging.info("GCP_LOCATION: " + GCP_LOCATION)

default_args = {
    "owner": "Michal Lebida",
    "depends_on_past": False,
    "email": ["michal.lebida@carouselgroup.net"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "dataflow_default_options": {
        "project": "cg-stg-us-co",
        "region": "us-central1",
        "zone": "us-central1",
        "tempLocation": f"gs://{GCS_BUCKET}/tmp/",
    },
}

with DAG(
    dag_id="archive-trading-documents",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["archive"],
) as dag:
    os.environ["JAVA_HOME"] = "/usr/bin/java"
    os.environ["PATH"] = os.environ["JAVA_HOME"] + ":" + os.environ["PATH"]
    run_archive = BeamRunJavaPipelineOperator(
        task_id="archive_trading_docs",
        jar=f"gs://{GCP_AIRFLOW_BUCKET}/dataflow-pipelines-1.0-SNAPSHOT.jar",
        job_class="net.carousel.couchbase.pipeline.CouchbaseArchivePipeline",
        runner="DataflowRunner",
        dataflow_config=dict(
            job_name=JOB_ENV + NOW + "archive-docs",
            project_id=GCP_PROJECT_ID,
            location=GCP_LOCATION,
            gcp_conn_id=GCP_CONNECTION_ID,
            wait_until_finished=True,
        ),
        pipeline_options=dict(
            configurationFile=JOB_ENV,
            env=JOB_ENV,
            universe="colorado",  # TODO make it configurable
            bucket="trading",
            outputFileName=ARCHIVE_OUT_FILE,
            stagingLocation=f"gs://{GCS_BUCKET}/temp/",
            gcpTempLocation=f"gs://{GCS_BUCKET}/temp/",
            saveHeapDumpsToGcsPath=f"gs://{GCS_BUCKET}/dump/",
            subnetwork=GCP_SUBNETWORK,
            network=GCP_NETWORK,
        ),
        gcp_conn_id=GCP_CONNECTION_ID,
        dag=dag,
    )
    # dataflow_status_checker = DataflowJobStatusSensor(
    #     task_id="wait-for-archive-process-to-finish",
    #     job_id="{{task_instance.xcom_pull('run_archive')['dataflow_job_id']}}",
    #     expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
    #     project_id=GCP_PROJECT_ID,
    #     location="us-central1",
    # )
    remove_archived_documents = BeamRunJavaPipelineOperator(
        task_id="drop_archive_trading_docs",
        jar=f"gs://{GCP_AIRFLOW_BUCKET}/dataflow-pipelines-1.0-SNAPSHOT.jar",
        job_class="net.carousel.couchbase.pipeline.RemoveMarkets",
        runner="DataflowRunner",
        dataflow_config=dict(
            job_name=JOB_ENV + NOW + "archive-docs-removal",
            project_id=GCP_PROJECT_ID,
            location=GCP_LOCATION,
            gcp_conn_id=GCP_CONNECTION_ID,
            wait_until_finished=True,
        ),
        pipeline_options=dict(
            configurationFile=JOB_ENV,
            env=JOB_ENV,
            universe="colorado",  # TODO make it configurable
            bucket="trading",
            stagingLocation=f"gs://{GCS_BUCKET}/temp/",
            gcpTempLocation=f"gs://{GCS_BUCKET}/temp/",
            saveHeapDumpsToGcsPath=f"gs://{GCS_BUCKET}/dump/",
            subnetwork=GCP_SUBNETWORK,
            network=GCP_NETWORK,
            inputFileName=ARCHIVE_OUT_FILE,
            documentType="nothing",
        ),
        gcp_conn_id=GCP_CONNECTION_ID,
        dag=dag,
    )
    run_archive >> remove_archived_documents
