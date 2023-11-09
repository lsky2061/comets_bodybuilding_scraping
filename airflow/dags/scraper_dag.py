from datetime import timedelta, date, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from contest_data_url_scraper import get_urls
from contest_data_scraper import get_contest_data
from scorecard_url_scraper import get_scorecard_urls

# from test_scorecard_ocr import scorecards_ocr

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 6),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "scraper_dag",
    default_args=default_args,
    description="Scrapes new contest URLs and contest results daily from NPC News Online.",
    schedule_interval=None,
)

run_url_scrape = PythonOperator(
    task_id="url_scrape",
    python_callable=get_urls,
    op_kwargs={"scrape_date": date.today()},
    dag=dag,
)

run_contest_results_scrape = PythonOperator(
    task_id="contest_results_scrape",
    python_callable=get_contest_data,
    dag=dag,
)

run_scorecard_url_scrape = PythonOperator(
    task_id="scorecard_url_scrape",
    python_callable=get_scorecard_urls,
    dag=dag,
)

push_urls_to_GCS = PostgresToGCSOperator(
    task_id="contesturls_to_cloud_storage",
    postgres_conn_id="postgres",
    sql='SELECT * FROM "contest_urls";',
    bucket="npc-news-online-data-pipeline-bucket",
    filename="contest_urls.csv",
    export_format="csv",
    gzip=False,
    use_server_side_cursor=False,
    dag=dag,
)

push_urls_to_BQ = GCSToBigQueryOperator(
    task_id="contesturls_to_bigquery",
    gcp_conn_id="google_cloud_default",
    bucket="npc-news-online-data-pipeline-bucket",
    source_objects=["contest_urls.csv"],
    source_format="CSV",
    destination_project_dataset_table="NPC_Contests.contest_urls",
    schema_fields=[
        {"name": "url", "type": "STRING", "mode": "NULLABLE"},
        {
            "name": "retrieved_timestamp",
            "type": "TIMESTAMP",
            "mode": "NULLABLE",
        },
        {
            "name": "results_are_scraped",
            "type": "BOOLEAN",
            "mode": "NULLABLE",
        },
    ],
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    skip_leading_rows=1,
    dag=dag,
)

push_results_to_GCS = PostgresToGCSOperator(
    task_id="contestresults_to_cloud_storage",
    postgres_conn_id="postgres",
    sql='SELECT * FROM "contest_results";',
    bucket="npc-news-online-data-pipeline-bucket",
    filename="contest_results.csv",
    export_format="csv",
    gzip=True,
    use_server_side_cursor=False,
    dag=dag,
)

push_results_to_BQ = GCSToBigQueryOperator(
    task_id="contestresults_to_bigquery",
    gcp_conn_id="google_cloud_default",
    bucket="npc-news-online-data-pipeline-bucket",
    source_objects=["contest_results.csv"],
    source_format="CSV",
    destination_project_dataset_table="NPC_Contests.contest_results",
    schema_fields=[
        {"name": "contest_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "competitor_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "competitor_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "organization", "type": "STRING", "mode": "NULLABLE"},
        {"name": "contest_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "contest_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "division", "type": "STRING", "mode": "NULLABLE"},
        {"name": "class", "type": "STRING", "mode": "NULLABLE"},
        {"name": "placing", "type": "INTEGER", "mode": "NULLABLE"},
        {
            "name": "scraped_timestamp",
            "type": "TIMESTAMP",
            "mode": "NULLABLE",
        },
        {
            "name": "is_loaded",
            "type": "BOOLEAN",
            "mode": "NULLABLE",
        },
    ],
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    skip_leading_rows=1,
    dag=dag,
)

push_scorecard_urls_to_GCS = PostgresToGCSOperator(
    task_id="contestscorecardurls_to_cloud_storage",
    postgres_conn_id="postgres",
    sql='SELECT * FROM "contest_scorecard_urls";',
    bucket="npc-news-online-data-pipeline-bucket",
    filename="contest_scorecard_urls.csv",
    export_format="csv",
    gzip=False,
    use_server_side_cursor=False,
    dag=dag,
)

push_scorecard_urls_to_BQ = GCSToBigQueryOperator(
    task_id="scorecardurls_to_bigquery",
    gcp_conn_id="google_cloud_default",
    bucket="npc-news-online-data-pipeline-bucket",
    source_objects=["contest_scorecard_urls.csv"],
    source_format="CSV",
    destination_project_dataset_table="NPC_Contests.contest_scorecard_urls",
    schema_fields=[
        {
            "name": "contest_scorecards_url",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {"name": "contest_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "post_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "scorecard_url", "type": "STRING", "mode": "NULLABLE"},
        {
            "name": "scraped_timestamp",
            "type": "TIMESTAMP",
            "mode": "NULLABLE",
        },
        {
            "name": "is_loaded",
            "type": "BOOLEAN",
            "mode": "NULLABLE",
        },
    ],
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    skip_leading_rows=1,
    dag=dag,
)

(
    run_url_scrape
    >> run_contest_results_scrape
    >> push_results_to_GCS
    >> push_results_to_BQ
)
run_url_scrape >> push_urls_to_GCS >> push_urls_to_BQ
run_scorecard_url_scrape >> push_scorecard_urls_to_GCS >> push_scorecard_urls_to_BQ
