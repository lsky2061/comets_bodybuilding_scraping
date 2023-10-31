from datetime import timedelta, date, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
from contest_data_url_scraper import get_urls
from contest_data_scraper import get_contest_data
from scorecard_url_scraper import get_scorecard_urls

# from test_scorecard_ocr import scorecards_ocr

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 24),
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

# run_scorecard_ocr = PythonOperator(
#     task_id="scorecard_ocr",
#     python_callable=scorecards_ocr,
#     dag=dag,
# )

(run_url_scrape >> run_contest_results_scrape)
(run_url_scrape >> run_scorecard_url_scrape)
# (run_scorecard_url_scrape >> run_scorecard_ocr)
# (run_contest_results_scrape >> run_scorecard_ocr)
