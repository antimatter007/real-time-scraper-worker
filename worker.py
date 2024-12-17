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
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_random_user_agent():
    """
    Generate a random User-Agent string using the Faker library.
    """
    fake = Faker()
    return fake.user_agent()

class RedditScraper:
    def __init__(self, proxy=None, timeout=10, random_user_agent=True):
        self.session = requests.Session()
        self.timeout = timeout
        self.proxy = proxy
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})

        if random_user_agent:
            self.session.headers.update({"User-Agent": get_random_user_agent()})
        else:
            # fallback static user-agent
            self.session.headers.update(
                {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/115.0.0.0 Safari/537.36"
                }
            )

    def search_reddit(self, query, limit=10):
        """
        Search Reddit for a given query and return up to `limit` posts.
        We rely on the public search endpoint: https://www.reddit.com/search.json
        """
        url = "https://www.reddit.com/search.json"
        params = {"q": query, "limit": limit, "sort": "relevance", "type": "link"}
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
            return []
        except Exception as e:
            logging.error(f"Error fetching search results: {e}")
            return []

        data = response.json()
        results = []
        for post in data.get("data", {}).get("children", []):
            post_data = post.get("data", {})
            results.append(
                {
                    "tweet_id": post_data.get("id"),
                    "tweet_text": post_data.get("title", ""),
                    "author_handle": post_data.get("author", ""),
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(post_data.get("created_utc", 0))),
                }
            )
        logging.info("Search returned %d results for query '%s'", len(results), query)
        return results

def store_results_in_db(conn, job_id, posts):
    """
    Insert results into the 'results' table.
    The table is assumed to have columns: job_id, tweet_id, tweet_text, author_handle, timestamp.
    """
    cur = conn.cursor()
    records = [(job_id, p['tweet_id'], p['tweet_text'], p['author_handle'], p['timestamp']) for p in posts]
    if records:
        query = """
            INSERT INTO results (job_id, tweet_id, tweet_text, author_handle, timestamp)
            VALUES %s
        """
        execute_values(cur, query, records)
    conn.commit()
    cur.close()

def update_job_status(conn, job_id, status):
    """
    Update the job status in the 'jobs' table.
    The 'jobs' table is assumed to have columns: id, status, updated_at.
    """
    cur = conn.cursor()
    cur.execute("UPDATE jobs SET status=%s, updated_at=NOW() WHERE id=%s", (status, job_id))
    conn.commit()
    cur.close()

def process_job(conn, job_id, query):
    """
    Process a single job:
    - Update job to 'in_progress'
    - Use RedditScraper to fetch posts
    - If no posts, mark as 'failed'
    - Else, store results and mark job as 'completed'
    """
    update_job_status(conn, job_id, 'in_progress')

    scraper = RedditScraper(random_user_agent=True)
    posts = scraper.search_reddit(query, limit=10)

    if not posts:
        logging.warning("No posts found or failed to fetch for query '%s'", query)
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

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode="require"
    )

    # Connect to RabbitMQ
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    def callback(ch, method, properties, body):
        message = json.loads(body)
        job_id = message['jobId']
        query = message['query']
        logging.info("Received job %s with query '%s'", job_id, query)

        process_job(conn, job_id, query)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)
    logging.info("Worker is waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()
