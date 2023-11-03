import psycopg2
from bs4 import BeautifulSoup
import requests
import itertools


def create_table(conn):
    # Create table in postgres
    with conn.cursor() as curs:
        curs.execute(
            """
            CREATE TABLE IF NOT EXISTS contest_urls
            (
                url TEXT PRIMARY KEY,
                retrieved_timestamp TIMESTAMP,
                results_are_scraped BOOLEAN
            )
            """
        )
        conn.commit()


def get_years_to_scrape(conn, kwargs):
    with conn.cursor() as curs:
        # Find the year of most recent scrape
        curs.execute(
            """
            SELECT MAX(substring(url,'.*/contests/([0-9]{4}).*'))
            FROM contest_urls
            """
        )
        last_year_scraped = curs.fetchone()[0]

    # If first scrape cycle, start at year 2012
    if last_year_scraped is None:
        last_year_scraped = 2012
    else:
        last_year_scraped = int(last_year_scraped)

    # Get year at time of scrape
    scrape_date = kwargs["scrape_date"]

    # Only check new URL additions for years since last scraped and now
    years = [str(year) for year in range(last_year_scraped, scrape_date.year + 1)]
    return years


def get_contest_urls(year, org):
    # Use with to close connection after response
    with requests.get(
        f"https://contests.npcnewsonline.com/contests/{year}/{org}"
    ) as response:
        content = response.content
    # Get HTML for page from specific year and organization
    parser = BeautifulSoup(content, "html.parser")

    # Contest page URLs are in div tags with contest-listing class
    contests = parser.find("div", class_="contest-listing").find_all("a")
    contest_urls = [contest.get("href") for contest in contests]
    return contest_urls


def check_if_scraped(conn, contest_url):
    with conn.cursor() as curs:
        # See if URL is already in table
        curs.execute(
            """
                SELECT COUNT(*)
                FROM contest_urls
                WHERE url=%s""",
            (contest_url,),
        )
        url_match_exists = curs.fetchone()[0]
    return url_match_exists


def insert_into_table(conn, contest_url):
    # Insert contest URL to table
    with conn.cursor() as curs:
        curs.execute(
            """
            INSERT INTO contest_urls
            (
                url,
                retrieved_timestamp,
                results_are_scraped
            )
            VALUES
            (
                %s, NOW(), %s
            )
            """,
            (contest_url, False),
        )
        conn.commit()


def get_urls(**kwargs):
    try:
        # Connect to Postgres database
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow",
            port=5432,
        )

        # Create table if not exists
        create_table(conn)

        # Get years to scrape
        years_to_scrape = get_years_to_scrape(conn, kwargs)

        # Organizations to run through
        organizations = ["ifbb", "npc", "npcw", "cpa"]

        # Where URLs will be saved
        contest_urls = []

        # Pages with contest URLs are by org and year
        for org, year in itertools.product(organizations, years_to_scrape):
            # Add contest URLs on page to contest_urls list
            contest_urls += get_contest_urls(year, org)

        # Add contest URL to table if not already there
        for contest_url in contest_urls:
            if check_if_scraped(conn, contest_url) == 0:
                insert_into_table(conn, contest_url)

    # Close Postgres connection
    finally:
        conn.close()
