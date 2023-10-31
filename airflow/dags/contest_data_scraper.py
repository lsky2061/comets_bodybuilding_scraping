import psycopg2
from bs4 import BeautifulSoup
import requests


def get_contest_data():
    try:
        # Connect to Postgres database
        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow",
            port=5432,
        )

        with conn.cursor() as curs:
            # Get URLs that haven't been scraped
            curs.execute(
                """
                SELECT url
                FROM contest_urls
                WHERE contest_urls.results_are_scraped = False
                """
            )
            url_list = curs.fetchall()

            # Create contest_results table
            curs.execute(
                """
                CREATE TABLE IF NOT EXISTS contest_results
                (
                    contest_url TEXT,
                    competitor_name TEXT,
                    competitor_url TEXT,
                    organization TEXT,
                    contest_name TEXT,
                    contest_date DATE,
                    division TEXT,
                    class TEXT,
                    "placing" SMALLINT,
                    scraped_timestamp TIMESTAMP,
                    is_loaded BOOLEAN
                )
                """
            )
            conn.commit()
        for url in url_list:
            url = url[0]

            # Use with to close connection after response
            with requests.get(url) as response:
                content = response.content

            # Get HTML content
            parser = BeautifulSoup(content, "html.parser")

            # Contest organization is IFBB, NPC, NPC Worldwide, or CPA
            contest_org = (
                parser.find("div", class_="two-thirds")
                .find("div", class_="category")
                .text
            )

            # Name of competition
            contest_name = parser.find(class_="entry-title").text

            # Date of competition
            contest_date = parser.find(class_="entry-date").text

            # Table with contest results
            contest_table = parser.find("table", class_="contest_table")

            for t in contest_table.find_all("td"):
                # Division is one of Men's Physique, Women's Figure, etc.
                division = t.find("h2")

                # Classes are Overall, Novice, Open Class A, etc.
                classes = t.find_all("div")

                classes_text = [class_text.text.strip() for class_text in classes]
                for class_text in classes_text:
                    # Class tags are nicknames for class
                    class_tag = class_text.replace(" ", "-")

                    # Find all competitors for given class
                    competitors = t.find_all(
                        attrs={"data-parent": class_tag, "data-person": "yes"}
                    )
                    for person in competitors:
                        # Skip link to comparison photos
                        if person.text.find("Comparisons") != -1:
                            continue

                        # Placing in span tags
                        placing = person.find("span").text

                        # Get name that appears after placing
                        competitor_name = person.text[len(placing) :]

                        # Get competitor's profile page
                        competitor_url = person["href"]

                        try:
                            # Competitors that competed
                            placing = int(placing)
                        except ValueError:
                            # Competitors that didn't compete
                            placing = None
                        # Insert competitor row into contest_results table
                        with conn.cursor() as curs:
                            curs.execute(
                                """
                                INSERT INTO contest_results
                                (
                                    contest_url,
                                    competitor_name,
                                    competitor_url,
                                    organization,
                                    contest_name,
                                    contest_date,
                                    division,
                                    class,
                                    "placing",
                                    scraped_timestamp,
                                    is_loaded
                                )
                                VALUES
                                (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), False
                                )
                                """,
                                (
                                    url,
                                    competitor_name.strip().lower(),
                                    competitor_url.strip().lower(),
                                    contest_org.strip().lower(),
                                    contest_name.strip().lower(),
                                    contest_date,
                                    division.text.strip().lower(),
                                    class_text.strip().lower(),
                                    placing,
                                ),
                            )
                            conn.commit()

                            # Update URL status to scraped
                            curs.execute(
                                """
                                UPDATE contest_urls
                                SET results_are_scraped=True
                                WHERE url=%s
                                """,
                                (url,),
                            )
                            conn.commit()

    finally:
        conn.close()
