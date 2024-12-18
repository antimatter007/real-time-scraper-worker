import json
import time
import logging
import requests
import pika
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import sys

# Set up logging to both file and stdout
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Remove any existing handlers to avoid duplication
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

    def scrape_search(self, query, limit=10):
        """
        Fetch up to `limit` posts related to `query` by using the Reddit search endpoint.
        URL: https://www.reddit.com/search.json?q=query&limit=10
        """
        logging.debug("Searching Reddit for query='%s', limit=%d", query, limit)
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
        posts = []
        children = data.get("data", {}).get("children", [])
        count = 0
        for c in children:
            if count >= limit:
                break
            post_data = c.get("data", {})
            post_id = post_data.get("name", f"post_{count}")  
            post_text = post_data.get("title", "No Title")
            author_handle = post_data.get("author", "unknown")
            created_utc = post_data.get("created_utc", 0)
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(created_utc))

            posts.append({
                "tweet_id": post_id,
                "tweet_text": post_text,
                "author_handle": author_handle,
                "timestamp": timestamp,
            })
            count += 1

        logging.info("Search returned %d results for query '%s'", len(posts), query)
        return posts

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
    posts = scraper.scrape_search(query, limit=10)

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

    # Hardcoded credentials (DO NOT CHANGE)
    RABBITMQ_URL = "amqps://pcudcyxc:CT6kMcrw_pXH7kFpqzpqWgoWnu5J04LU@duck.lmq.cloudamqp.com/pcudcyxc"
    QUEUE_NAME = "jobs_queue"

    PG_HOST = "autorack.proxy.rlwy.net"
    PG_PORT = "20823"
    PG_DB = "railway"
    PG_USER = "postgres"
    PG_PASSWORD = "suFzdtdvTXFdhgQloNbxzOHMjLsisThP"

    logging.debug("Connecting to PostgreSQL at %s:%s db=%s user=%s", PG_HOST, PG_PORT, PG_DB, PG_USER)
    print("Connecting to PostgreSQL...")  
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

    logging.debug("Connecting to RabbitMQ using URL: %s", RABBITMQ_URL)
    print("Connecting to RabbitMQ...")
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
            # Acknowledge to avoid infinite loop
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            process_job(conn, job_id, query)
        except Exception as e:
            logging.error("Error processing job %s: %s", job_id, e)
            print(f"Error processing job {job_id}:", e)
            # Mark job failed if needed
            update_job_status(conn, job_id, 'failed')

        # Acknowledge message
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
