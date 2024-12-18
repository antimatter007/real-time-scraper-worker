import json
import time
import logging
import requests
import pika
import psycopg2
from psycopg2.extras import execute_values
import sys
import random
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from session import RandomUserAgentSession

# Logging configuration
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

for h in logger.handlers[:]:
    logger.removeHandler(h)

file_handler = logging.FileHandler("worker.log")
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(file_formatter)
logger.addHandler(stream_handler)

print("Worker script started - initializing...")

class YARS:
    __slots__ = ("session", "proxy", "timeout")

    def __init__(self, proxy=None, timeout=10):
        self.session = RandomUserAgentSession()
        self.proxy = proxy
        self.timeout = timeout

        retries = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})

    def handle_search(self, url, params, after=None, before=None):
        if after:
            params["after"] = after
        if before:
            params["before"] = before

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            logging.info("Search request successful")
        except requests.exceptions.HTTPError as e:
            if response.status_code != 200:
                logging.info("Search request unsuccessful due to: %s", e)
                print(f"Failed to fetch search results: {response.status_code}")
                return []
        except Exception as e:
            logging.info("Search request error: %s", e)
            return []

        data = response.json()
        results = []
        children = data.get("data", {}).get("children", [])
        for post in children:
            post_data = post.get("data", {})
            tweet_id = post_data.get("name", "no_id")
            tweet_text = post_data.get("title", "No Title")
            author_handle = post_data.get("author", "unknown")
            created_utc = post_data.get("created_utc", 0)
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(created_utc))

            results.append({
                "tweet_id": tweet_id,
                "tweet_text": tweet_text,
                "author_handle": author_handle,
                "timestamp": timestamp,
            })

        logging.info("Search Results Returned %d Results", len(results))
        return results

    def search_reddit(self, query, limit=10, after=None, before=None):
        url = "https://www.reddit.com/search.json"
        params = {"q": query, "limit": limit, "sort": "relevance", "type": "link"}
        return self.handle_search(url, params, after, before)

RABBITMQ_URL = "amqps://pcudcyxc:CT6kMcrw_pXH7kFpqzpqWgoWnu5J04LU@duck.lmq.cloudamqp.com/pcudcyxc"
QUEUE_NAME = "jobs_queue"

PG_HOST = "autorack.proxy.rlwy.net"
PG_PORT = "20823"
PG_DB = "railway"
PG_USER = "postgres"
PG_PASSWORD = "suFzdtdvTXFdhgQloNbxzOHMjLsisThP"

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

    scraper = YARS()
    posts = scraper.search_reddit(query, limit=10)

    if not posts:
        logging.warning("No posts found or fetch failed for query '%s' (Job %d)", query, job_id)
        update_job_status(conn, job_id, 'failed')
        return

    store_results_in_db(conn, job_id, posts)
    update_job_status(conn, job_id, 'completed')
    logging.info("Job %d completed successfully with %d posts.", job_id, len(posts))

def main():
    print("Entering main function...")
    logging.debug("Entering main function...")

    print("Connecting to PostgreSQL...")
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
        print("PostgreSQL connection established.")
    except Exception as e:
        logging.error("Failed to connect to PostgreSQL: %s", e)
        print("Failed to connect to PostgreSQL.", e)
        sys.exit(1)

    print("Connecting to RabbitMQ...")
    logging.debug("Connecting to RabbitMQ using URL: %s", RABBITMQ_URL)
    try:
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        logging.debug("RabbitMQ connection established and queue declared.")
        print("RabbitMQ connected and queue declared.")
    except Exception as e:
        logging.error("Failed to connect to RabbitMQ or declare queue: %s", e)
        print("Failed to connect to RabbitMQ or declare queue.", e)
        sys.exit(1)

    def callback(ch, method, properties, body):
        logging.debug("Received a message from RabbitMQ. Delivery tag: %s", method.delivery_tag)
        print(f"Message received with delivery tag: {method.delivery_tag}")
        try:
            message = json.loads(body)
            job_id = message['jobId']
            query = message['query']
            logging.info("Received job %s with query '%s'", job_id, query)
            print(f"Received job {job_id} with query '{query}'")
        except Exception as e:
            logging.error("Failed to parse message: %s", e)
            print("Failed to parse message:", e)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            process_job(conn, job_id, query)
        except Exception as e:
            logging.error("Error processing job %s: %s", job_id, e)
            print(f"Error processing job {job_id}:", e)
            update_job_status(conn, job_id, 'failed')

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.debug("Job %s acknowledged and completed", job_id)
        print(f"Job {job_id} done and acked")

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)
    logging.info("Worker is waiting for messages. Starting consuming loop...")
    print("Worker is waiting for messages. Starting consuming loop...")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Worker was interrupted by KeyboardInterrupt, shutting down.")
        print("KeyboardInterrupt received, stopping consumer.")
        channel.stop_consuming()
    except Exception as e:
        logging.error("Unexpected error in consuming loop: %s", e)
        print("Unexpected error in consuming loop:", e)
    finally:
        logging.debug("Closing RabbitMQ connection.")
        print("Closing RabbitMQ connection.")
        connection.close()

if __name__ == "__main__":
    print("Worker entrypoint executing...")
    logging.debug("Worker entrypoint executing...")
    main()
