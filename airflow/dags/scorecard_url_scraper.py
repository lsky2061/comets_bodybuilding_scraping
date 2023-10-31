import psycopg2
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import numpy as np


def create_table(conn):
    # Create contest_results table
    with conn.cursor() as curs:
        curs.execute(
            """
            CREATE TABLE IF NOT EXISTS contest_scorecard_urls
            (
                contest_url TEXT,
                scorecard_url TEXT,
                scraped_timestamp TIMESTAMP,
                ocr_is_complete BOOLEAN
            )
            """
        )
        conn.commit()


def get_new_contest_urls(conn):
    with conn.cursor() as curs:
        curs.execute(
            """
            SELECT url
            FROM contest_urls
            WHERE contest_urls.scorecards_are_scraped = False
                AND contest_urls.scorecards_exist = True
            """
        )
        url_list = curs.fetchall()
    return url_list


def get_contest_and_year(contest_url):
    url_temp = contest_url[0]
    contest = url_temp.split("/")[-1].replace("_", "-")
    year = url_temp.split("/")[-2]
    return contest, year


def get_scorecard_page_html(contest, year):
    scorecard_url = (
        "https://npcnewsonline.com/" + year + "-" + contest + "-official-score-cards"
    )
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"
    }

    # Use with to close connection after response
    with requests.get(scorecard_url, headers=headers) as response:
        time.sleep(1.0 + np.random.uniform(0, 1))
        content = response.content

    # Get HTML content
    parser = BeautifulSoup(content, "html.parser")
    return parser


def update_scrape_status(conn, contest_url):
    with conn.cursor() as curs:
        curs.execute(
            """
            UPDATE contest_urls
            SET scorecards_exist = True
                AND scorecards_are_scraped=True
            WHERE url=%s
            """,
            (contest_url,),
        )
        conn.commit()


def update_scorecard_exist_status(conn, contest_url):
    with conn.cursor() as curs:
        curs.execute(
            """
            UPDATE contest_urls
            SET scorecards_exist = False
            WHERE url=%s
            """,
            (contest_url,),
        )
        conn.commit()


def get_image_urls(main_content):
    # Find scorecard img urls in a href or img tags
    scorecard_image_urls = [tag.get("data-src") for tag in main_content.find_all("img")]
    return scorecard_image_urls


def insert_image_url(conn, contest_url, image_url):
    # Add row to table with image url
    with conn.cursor() as curs:
        curs.execute(
            """
            INSERT INTO contest_scorecard_urls
            (
                contest_url,
                scorecard_url,
                scraped_timestamp,
                ocr_is_complete
            )
            VALUES
            (
                %s, %s, NOW(), False
            )
            """,
            (contest_url, image_url),
        )
        conn.commit()


def get_scorecard_urls():
    try:
        # Connect to Postgres database
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow",
            port=5432,
        )

        # Create table if it doesn't exist
        create_table(conn)

        # Get new contest URLs from Postgres
        contest_urls = get_new_contest_urls(conn)

        count = 0
        # Scorecard URL based on contest URL
        for contest_url in contest_urls:
            contest, year = get_contest_and_year(contest_url)
            # Scorecards were posted starting in 2015
            if int(year) < 2015:
                continue
            parser = get_scorecard_page_html(contest, year)

            # Get main content section from scorecard page
            main_content = parser.find("div", class_="entry-content")

            # Main content section exists if scorecards exist
            if main_content:
                scorecard_image_urls = get_image_urls(main_content)
                for image_url in scorecard_image_urls:
                    insert_image_url(conn, contest_url, image_url)

                count += 1

                # Update scrape status even if no scorecards found
                update_scrape_status(conn, contest_url)

            # If scorecards don't exist update Postgres table
            else:
                print("Scorecards do not exist for", contest_url)
                update_scorecard_exist_status(conn, contest_url)

        print(count, "contests of", len(contest_urls), "have scorecards")

    finally:
        conn.close()
