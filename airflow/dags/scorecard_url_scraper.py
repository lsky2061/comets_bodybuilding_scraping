import psycopg2
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
from datetime import datetime
import numpy as np


def create_table(conn):
    # Create contest_results table
    with conn.cursor() as curs:
        curs.execute(
            """
            CREATE TABLE IF NOT EXISTS contest_scorecard_urls
            (
                contest_scorecards_url TEXT,
                contest_name TEXT,
                post_date TIMESTAMP,
                scorecard_url TEXT,
                scraped_timestamp TIMESTAMP,
                is_loaded BOOLEAN
            )
            """
        )
        conn.commit()


def get_last_scraped_post_date(conn):
    # Getting post date of most recently scraped scorecards
    with conn.cursor() as curs:
        curs.execute(
            """
            SELECT MAX(post_date)
            FROM contest_scorecard_urls
            """
        )
        last_scraped_post_date = curs.fetchone()[0]
    if last_scraped_post_date:
        return datetime.fromtimestamp(last_scraped_post_date)
    else:
        return datetime.strptime("1000-01-01", "%Y-%m-%d").date()


def get_scorecard_list(page_url=None, page_number=None):
    headers = {
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
    }
    if page_number:
        page_url = (
            "https://npcnewsonline.com/category/contest-scorecards/page/"
            + f"{page_number}"
        )
    print(page_url)
    # Use with to close connection after response
    with requests.get(
        page_url,
        headers=headers,
    ) as response:
        time.sleep(1.0 + np.random.uniform(0, 1))
        content = response.content

    # Get HTML content
    parser = BeautifulSoup(content, "html.parser")
    print(parser)
    main_content = parser.find("main")
    if main_content:
        posts = main_content.find_all("a", {"rel": "bookmark"})
        return posts
    else:
        return None


def get_post_dates(posts):
    post_dates = [post.find("time").text for post in posts]
    formatted_post_dates = [
        datetime.strptime(post_date, "%B %d, %Y").date() for post_date in post_dates
    ]
    return formatted_post_dates


def get_scorecard_page_urls(posts):
    contest_scorecard_page_urls = [post["href"] for post in posts]
    return contest_scorecard_page_urls


def get_contest_scorecard_page_content(contest_scorecards_url):
    with requests.get(contest_scorecards_url) as response:
        time.sleep(1.0 + np.random.uniform(0, 1))
        content = response.content

    # Get HTML content
    parser = BeautifulSoup(content, "html.parser")
    main_content = parser.find("main")
    return main_content


def get_contest_name(main_content):
    contest_name_temp = main_content.find("h1", class_="entry-title").text
    contest_name_temp = contest_name_temp.split("Official")[0]
    contest_name_temp = contest_name_temp.split("Day")[0]
    contest_name = contest_name_temp.split("Score")[0]
    return contest_name.strip().lower()


def get_image_urls(main_content):
    entry_content = main_content.find("div", class_="entry-content")
    # Find scorecard img urls in a href or img tags
    scorecard_image_urls = [
        tag.get("data-src") for tag in entry_content.find_all("img")
    ]
    return scorecard_image_urls


def insert_image_url(conn, contest_url, contest_name, post_date, image_url):
    # Add row to table with image url
    with conn.cursor() as curs:
        curs.execute(
            """
            INSERT INTO contest_scorecard_urls
            (
                contest_scorecards_url,
                contest_name,
                post_date,
                scorecard_url,
                scraped_timestamp,
                is_loaded
            )
            VALUES
            (
                %s, %s, %s, %s, NOW(), False
            )
            """,
            (contest_url, contest_name, post_date, image_url),
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

        # Get post date of last scraped scorecard page
        last_scraped_post_date = get_last_scraped_post_date(conn)

        scorecard_posts = get_scorecard_list(
            page_url="https://npcnewsonline.com/category/contest-scorecards"
        )
        contest_scorecard_urls = get_scorecard_page_urls(scorecard_posts)
        contest_scorecard_dates = get_post_dates(scorecard_posts)

        page_number = 2

        while contest_scorecard_dates[-1] >= last_scraped_post_date:
            page_number += 1
            scorecard_posts = get_scorecard_list(page_number=page_number)
            if scorecard_posts:
                contest_scorecard_urls += get_scorecard_page_urls(scorecard_posts)
                contest_scorecard_dates += get_post_dates(scorecard_posts)
            else:
                break

        # Scorecard URL based on contest URL
        for idx, contest_url in enumerate(contest_scorecard_urls):
            post_date = contest_scorecard_dates[idx]
            # Get main content of contest scorecard page
            main_content = get_contest_scorecard_page_content(contest_url)
            contest_name = get_contest_name(main_content)
            # Main content section exists if scorecards exist
            if main_content:
                scorecard_image_urls = get_image_urls(main_content)
                print("Inserting images for " + contest_name)
                for image_url in scorecard_image_urls:
                    insert_image_url(
                        conn, contest_url, contest_name, post_date, image_url
                    )

    finally:
        conn.close()
