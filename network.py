"""
Module for network operations such as requests, http sessions, custom classes etc.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional
import time

class FixedDelayRetry(Retry):
    """Custom Retry class with a fixed time delay."""
    def __init__(self, *args, delay, **kwargs):
        super().__init__(*args, **kwargs)
        self.delay = delay

    def sleep(self, *args, **kwargs):
        """Overrides the default sleep behavior with a fixed time delay."""
        print(f"Waiting {self.delay} seconds before next retry...")
        time.sleep(self.delay)


def create_http_session(fixed_delay_retry: Optional[int] = None,backoff_factor: int = 1) -> requests.session:
    """
    Creates the HTTP session to be used across all data scrapings.
    It implements an exponential backoff to improve robustness and in order not to overload the target website.
    The exponential backoff can be overriden by setting it to 0 to adhere a specific data source crawler requirement found in robots.txt
    """

    session = requests.Session()

    if fixed_delay_retry:
        retry_strategy = FixedDelayRetry(
            total=5,
            delay=fixed_delay_retry,
            status_forcelist=[429, 404, 500, 502, 503, 504]
        )

    else:
        
        retry_strategy = Retry(
            total=5, 
            backoff_factor=backoff_factor,  # Exponential delay backoff
            status_forcelist=[500, 404, 502, 503, 504],  # HTTP status codes to retry on
            #method_whitelist=["HEAD", "GET", "OPTIONS"]  # HTTP methods to retry on
        )

    # Mount the adapter with the retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session