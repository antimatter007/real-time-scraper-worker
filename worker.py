import json
import time
import logging
import requests
import pika
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

logging.basicConfig(
    filename="worker.log",
    level=logging.DEBUG,  # Set to DEBUG to capture detailed logs
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def get_random_user_agent():
    fake = Faker()
    ua = fake.user_agent()
    logging.debug(f"Generated random User-Agent: {ua}")
    return ua

class RedditScraper:
    def __init__(self, proxy=None, timeout=10, random_user_agent=True):
        logging.debug("Initializing RedditScraper")
        self.session = requests.Session()
        self.timeout = timeout
        self.proxy = proxy
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})
            logging.debug(f"Using proxy: {proxy}")

        if random_user_agent:
            ua = get_random_user_agent()
            self.session.headers.update({"User-Agent": ua})
        else:
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/115.0.0.0 Safari/537.36"
            })
        logging.debug("RedditScraper initialized with headers: %s", self.session.headers)

    def search_reddit(self, query, limit=10):
        logging.debug(f"Starting Reddit search for query: {query}, limit: {limit}")
        url = "https://www.reddit.com/search.json"
        params = {"q": query, "limit": limit, "sort": "relevance", "type": "link"}
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error during search: {http_err}")
            return []
        except Exception as e:
            logging.error(f"Error fetching search results: {e}")
            return []

        data = response.json()
        results = []
        for post in data.get("data", {}).get("children", []):
            post_data = post.get("data", {})
            results.append({
                "tweet_id": post_data.get("id"),
                "tweet_text": post_data.get("title", ""),
                "author_handle": post_data.get("author", ""),
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S', 
                           time.gmtime(post_data.get("created_utc", 0))),
            })
        logging.info("Search returned %d results for query '%s'", len(results), query)
        return results

def store_results_in_db(conn, job_id, posts):
    logging.debug(f"Storing {len(posts)} posts in DB for job_id={job_id}")
    cur = conn.cursor()
    records = [(job_id, p['tweet_id'], p['tweet_text'], p['author_handle'], p['timestamp']) for p in posts]
    if records:
        query = """
            INSERT INTO results (job_id, tweet_id, tweet_text, author_handle, timestamp)
            VALUES %s
        """
        try:
            execute_values(cur, query, records)
            conn.commit()
            logging.debug("Inserted posts into results table successfully")
        except Exception as e:
            logging.error(f"Error inserting posts into DB: {e}")
            conn.rollback()
    else:
        logging.debug("No records to store, skipping DB insert.")
    cur.close()

def update_job_status(conn, job_id, status):
    logging.debug(f"Updating job {job_id} to status '{status}'")
    cur = conn.cursor()
    try:
        cur.execute("UPDATE jobs SET status=%s, updated_at=NOW() WHERE id=%s", (status, job_id))
        conn.commit()
        logging.debug("Job status updated successfully")
    except Exception as e:
        logging.error(f"Error updating job status in DB: {e}")
        conn.rollback()
    cur.close()

def process_job(conn, job_id, query):
    logging.info(f"Processing job {job_id} with query '{query}'")
    update_job_status(conn, job_id, 'in_progress')

    scraper = RedditScraper(random_user_agent=True)
    posts = scraper.search_reddit(query, limit=10)

    if not posts:
        # no posts found or fetch failed
        logging.warning(f"No posts found or fetch failed for query '{query}'")
        update_job_status(conn, job_id, 'failed')
        return

    store_results_in_db(conn, job_id, posts)
    update_job_status(conn, job_id, 'completed')
    logging.info(f"Job {job_id} completed successfully with {len(posts)} posts.")

def main():
    # Hardcoded credentials for RabbitMQ and PostgreSQL
    # Update these with your actual credentials/hosts
    RABBITMQ_URL = "amqps://pcudcyxc:CT6kMcrw_pXH7kFpqzpqWgoWnu5J04LU@duck.lmq.cloudamqp.com/pcudcyxc"
    QUEUE_NAME = "jobs_queue"

    PG_HOST = "autorack.proxy.rlwy.net"
    PG_PORT = "20823"
    PG_DB = "railway"
    PG_USER = "postgres"
    PG_PASSWORD = "suFzdtdvTXFdhgQloNbxzOHMjLsisThP"

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode="require"
    )

    logging.debug("PostgreSQL connection established")

    logging.debug("Connecting to RabbitMQ")
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    logging.debug("RabbitMQ connection established and queue declared")

    def callback(ch, method, properties, body):
        logging.debug("Received a message from RabbitMQ")
        try:
            message = json.loads(body)
            job_id = message['jobId']
            query = message['query']
            logging.info(f"Received job {job_id} with query '{query}'")
        except Exception as e:
            logging.error(f"Failed to parse message: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        process_job(conn, job_id, query)

        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.debug(f"Job {job_id} acknowledged and completed")

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)
    logging.info("Worker is waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()