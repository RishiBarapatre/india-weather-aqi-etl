"""
include/utils/retry.py
-----------------------
Shared retry decorator built on the `tenacity` library.

Why tenacity instead of a simple for-loop?
  - Exponential back-off: waits 2s, 4s, 8s… instead of hammering the API
    every second, which could get our IP rate-limited or banned.
  - Jitter: adds a small random offset to each wait so that if multiple
    Airflow tasks retry simultaneously, they don't all hit the API at the
    exact same moment (thundering herd problem).
  - Structured logging: tenacity calls our before_sleep callback on every
    retry, giving us a clear log trail of which attempt failed and why.
  - Declarative: the retry policy is expressed as a decorator, keeping the
    actual business logic in the calling function clean.

Usage:
    from include.utils.retry import retry_on_http_error

    @retry_on_http_error
    def fetch_something():
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
"""

import logging

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    wait_random,
    retry_if_exception_type,
    before_sleep_log,
    RetryError,
)

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# The retry decorator
# ---------------------------------------------------------------------------
retry_on_http_error = retry(
    # Which exceptions should trigger a retry?
    # Only transient network / server errors — not programming mistakes.
    retry=retry_if_exception_type((
        requests.exceptions.ConnectionError,   # network down / DNS failure
        requests.exceptions.Timeout,           # request took too long
        requests.exceptions.HTTPError,         # 5xx server errors
    )),

    # How many total attempts? (1 original + 3 retries = 4 total)
    stop=stop_after_attempt(4),

    # How long to wait between attempts?
    # wait_exponential: 2s → 4s → 8s (doubles each time, capped at 30s)
    # wait_random:      adds 0–2s of jitter on top
    wait=wait_exponential(multiplier=1, min=2, max=30) + wait_random(0, 2),

    # Log a warning before each sleep so we can see retries in Airflow logs
    before_sleep=before_sleep_log(logger, logging.WARNING),

    # Don't suppress the final exception — let it propagate so Airflow
    # marks the task as failed and triggers its own retry logic
    reraise=True,
)


__all__ = ["retry_on_http_error", "RetryError"]
