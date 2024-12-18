import json
import time
import logging
import requests
import pika
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import sys

logging.basicConfig(
    filename="worker.log",
    level=logging.DEBUG,  # DEBUG level for detailed information
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def get_random_user_agent():
    fake = Faker()
    ua = fake.user_agent()
    logging.debug("Generated random User-Agent: %s", ua)
    return ua

class RedditScraper:
    def __init__(self, proxy=None, timeout=10, random_user_agent=True):
        logging.debug("Initializing RedditScraper...")
        self.session = requests.Session()
        self.timeout = timeout
        self.proxy = proxy
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})
            logging.debug("Using proxy: %s", proxy)

        if random_user_agent:
            ua = get_random_user_agent()
            self.session.headers.update({"User-Agent": ua})
        else:
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/115.0.0.0 Safari/537.36"
            })

        logging.debug("RedditScraper initialized. Headers: %s", self.session.headers)

    def search_reddit(self, query, limit=10):
        logging.debug("Starting Reddit search for query='%s', limit=%d", query, limit)
        url = "https://www.reddit.com/search.json"
        params = {"q": query, "limit": limit, "sort": "relevance", "type": "link"}
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            logging.error("HTTP error during search: %s", http_err)
            return []
        except Exception as e:
            logging.error("Error fetching search results: %s", e)
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
    logging.debug("Storing %d posts in DB for job_id=%d", len(posts), job_id)
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
            logging.debug("Inserted posts into results table successfully.")
        except Exception as e:
            logging.error("Error inserting posts into DB: %s", e)
            conn.rollback()
    else:
        logging.debug("No records to store, skipping DB insert.")
    cur.close()

def update_job_status(conn, job_id, status):
    logging.debug("Updating job %d to status '%s'", job_id, status)
    cur = conn.cursor()
    try:
        cur.execute("UPDATE jobs SET status=%s, updated_at=NOW() WHERE id=%s", (status, job_id))
        conn.commit()
        logging.debug("Job status updated successfully.")
    except Exception as e:
        logging.error("Error updating job status in DB: %s", e)
        conn.rollback()
    cur.close()

def process_job(conn, job_id, query):
    logging.info("Processing job %d with query '%s'", job_id, query)
    update_job_status(conn, job_id, 'in_progress')

    scraper = RedditScraper(random_user_agent=True)
    posts = scraper.search_reddit(query, limit=10)

    if not posts:
        logging.warning("No posts found or fetch failed for query '%s' (Job %d)", query, job_id)
        update_job_status(conn, job_id, 'failed')
        return

    store_results_in_db(conn, job_id, posts)
    update_job_status(conn, job_id, 'completed')
    logging.info("Job %d completed successfully with %d posts.", job_id, len(posts))

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

    logging.debug("Connecting to PostgreSQL at %s:%s db=%s user=%s", PG_HOST, PG_PORT, PG_DB, PG_USER)
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            sslmode="require"
        )
        logging.debug("PostgreSQL connection established.")
    except Exception as e:
        logging.error("Failed to connect to PostgreSQL: %s", e)
        sys.exit(1)

    logging.debug("Connecting to RabbitMQ using URL: %s", RABBITMQ_URL)
    try:
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        logging.debug("RabbitMQ connection established and queue declared.")
    except Exception as e:
        logging.error("Failed to connect to RabbitMQ or declare queue: %s", e)
        sys.exit(1)

    def callback(ch, method, properties, body):
        logging.debug("Received a message from RabbitMQ. Delivery tag: %s", method.delivery_tag)
        try:
            message = json.loads(body)
            job_id = message['jobId']
            query = message['query']
            logging.info("Received job %s with query '%s'", job_id, query)
        except Exception as e:
            logging.error("Failed to parse message: %s", e)
            # Even if parse fails, acknowledge to not requeue and cause infinite loop
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            process_job(conn, job_id, query)
        except Exception as e:
            logging.error("Error processing job %s: %s", job_id, e)
            # Mark job failed if desired or leave as is
            update_job_status(conn, job_id, 'failed')

        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.debug("Job %s acknowledged and completed", job_id)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)
    logging.info("Worker is waiting for messages. Starting consuming loop...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Worker was interrupted by KeyboardInterrupt, shutting down.")
        channel.stop_consuming()
    except Exception as e:
        logging.error("Unexpected error in consuming loop: %s", e)
    finally:
        logging.debug("Closing RabbitMQ connection.")
        connection.close()

if __name__ == "__main__":
    main()
