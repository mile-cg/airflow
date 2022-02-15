from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.models import Variable

GCP_CONNECTION_ID = "GCP"
POSTGRES_CONNECTION_ID = "PSQL_PROMOTIONS"
SQL_QUERY = "SELECT * from promotion_granted"
GCS_BUCKET = Variable.get("GCP_AIRFLOW_BUCKET")
FILENAME = "promotion_cache_dump"
DATASET_NAME = "bonus_cache"
TABLE_NAME = "promotions_granted"

dag = DAG(
    dag_id="bonus_cache_export",
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["bonus"],
)

upload_data_server_side_cursor = PostgresToGCSOperator(
    task_id="promotions_granted",
    sql=SQL_QUERY,
    bucket=GCS_BUCKET,
    filename=FILENAME,
    gzip=False,
    use_server_side_cursor=True,
    dag=dag,
    postgres_conn_id=POSTGRES_CONNECTION_ID,
    gcp_conn_id=GCP_CONNECTION_ID,
)

load_csv = GCSToBigQueryOperator(
    task_id="promotions_gcs_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=FILENAME,
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "promo_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ],
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
    bigquery_conn_id=GCP_CONNECTION_ID,
    google_cloud_storage_conn_id=GCP_CONNECTION_ID,
    location="us-central1",
    source_format="NEWLINE_DELIMITED_JSON",
)

end_task = BashOperator(
    task_id="print_end",
    bash_command='echo "FINISH success"',
)

upload_data_server_side_cursor >> load_csv >> end_task

if __name__ == "__main__":
    dag.cli()
