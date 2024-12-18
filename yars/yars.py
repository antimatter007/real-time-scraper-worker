# yars.py
from __future__ import annotations
from .sessions import RandomUserAgentSession
import time
import random
import logging
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

logging.basicConfig(
    filename="YARS.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class YARS:
    __slots__ = ("session", "proxy", "timeout")

    def __init__(self, proxy=None, timeout=10, random_user_agent=True):
        self.session = RandomUserAgentSession() if random_user_agent else requests.Session()
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
        except Exception as e:
            if hasattr(response, 'status_code') and response.status_code != 200:
                logging.info("Search request unsuccessful due to: %s", e)
                print(f"Failed to fetch search results: {response.status_code}")
            return []

        data = response.json()
        results = []
        children = data.get("data", {}).get("children", [])
        for post in children:
            post_data = post.get("data", {})
            # Extract the fields we want:
            tweet_id = post_data.get("name", "no_id")  # e.g. 't3_xyz'
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

    def search_subreddit(self, subreddit, query, limit=10, after=None, before=None, sort="relevance"):
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
        params = {"q": query, "limit": limit, "sort": "relevance", "type": "link","restrict_sr":"on"}
        return self.handle_search(url, params, after, before)

    # The rest of the methods (scrape_post_details, scrape_user_data, fetch_subreddit_posts) remain unchanged if not needed for current logic
