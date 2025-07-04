from datetime import datetime
from dateutil import parser
from functools import lru_cache
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Callable, Optional, Union


import atproto

from .utils import (
    _safe_json,
    dump_to_file,
    finalize_item_processing,
    flatten_json,
    from_cli,
    get_author_data,
    get_client,
    get_hashtags,
    get_mentions,
    get_post_data,
    get_target_data,
    get_type,
    has_link_,
    has_term,
    make_cached_fetchers,
    master_filter,
    normalize_cutoff_time,
    retry,
    structure_item,
    validate_type_filter,
)


class historicalQuery:
    def __init__(
        self,
        handle:       Optional[str]            = None,
        app_password: Optional[str]            = None,
        client:       Optional[atproto.Client] = None,
        get_author_data_fn: Optional[Callable] = None,
        get_post_data_fn:   Optional[Callable] = None
    ):
        self.client = get_client(handle, app_password, client)

        if get_author_data_fn is not None and get_post_data_fn is not None: # <- EXECUTED FROM CLI
            self.get_author_data_cached  = get_author_data_fn
            self.get_post_data_cached    = get_post_data_fn
        else:
            raw_get_author, raw_get_post = make_cached_fetchers(self.client)
            self.get_author_data_cached  = lru_cache(maxsize=1024)(raw_get_author)
            self.get_post_data_cached    = lru_cache(maxsize=1024)(raw_get_post)

        self.item_counter = {'count': 0}

    def _search_posts_page(self, filter_term, cursor, limit):
        return self.client.app.bsky.feed.search_posts({
            "q": filter_term,
            "cursor": cursor,
            "limit": limit
        })

    def _convert_post_to_item(self, post_data: dict) -> dict:
        uri = post_data.get('uri')
        collection = post_data['record'].get('py_type')
        rkey = re.search(r'at://[^/]+/[^/]+/([^/]+)', uri).group(1)

        return {
            'repo':        post_data['author'].get('did'),
            'action':      'create',
            'uri':         uri,
            'cid':         post_data['record'].get('cid'),
            'collection':  collection,
            'path':        f'{collection}/{uri}',
            'record':      post_data.get('record'),
            'action_type': get_type(collection, 'create', post_data.get('record')),
            'post_data':   post_data
        }

    def _handle_item(self, item, to_row, dump_kwargs, results) -> None:
        if not dump_kwargs:
            self.item_counter['count'] += 1
            item_structured = finalize_item_processing(item, self.get_author_data_cached, self.get_post_data_cached)
            if to_row:
                results.append(flatten_json(item_structured))
            else:
                results.append(item_structured)
            print(f'{self.item_counter['count']} items processed', end='\r', flush=True)
        else:
            dump_to_file(item, item_counter = self.item_counter, **dump_kwargs)


    def retry (method, params, retries=5):
        delay = 5
        while retries > 0:
            try:
                r = method (params)
                return r
            except:
                print ("    error, sleeping " + str (delay) + "s")
                time.sleep (delay)
                delay = delay * 2
                retries = retries - 1
        return None


    def retry(
        fn: Callable,
        params,
        retries: int = 5,
        delay: float = 5.0,
        backoff: float = 2.0,
        exceptions: Tuple[Type[BaseException], ...] = (Exception,),
        logger: logging.Logger = logging.getLogger(__name__)
    ):
        """
        Retry fn(*args) with exponential backoff.
        - retries: max attempts
        - delay: initial wait
        - backoff: multiplier for each retry
        - exceptions: exception types to catch
        """
        attempt = 1
        while attempt <= retries:
            try:
                return fn(params)
            except exceptions as e:
                if attempt == retries:
                    logger.error("All %d retries failed: %s", retries, e)
                    raise
                sleep_time = delay * (backoff ** (attempt - 1))
                # add jitter of ±10%
                jitter = random.uniform(0.9, 1.1)  
                actual_sleep = sleep_time * jitter
                logger.warning(
                    "Attempt %d/%d failed (%s), sleeping %.1fs",
                    attempt, retries, e, actual_sleep
                )
                time.sleep(actual_sleep)
                attempt += 1


    def loop_search_posts(
        query: str,
        client,
        limit: int = 50000,
        page_size: int = 100,
        sleep_secs: float = 1.0
    ) -> List[Dict[str, Any]]:
        """
        Iteratively search Bluesky posts matching `query`, handling both
        cursor‐based pagination and recursive 'until:' backfills when
        cursors disappear. Returns up to `limit` unique posts (by 'uri').
        """
        rows: List[Any] = []
        cursor: Optional[str] = None
        until: Optional[str] = None

        while len(rows) < limit:
            # Build query (add until: when backfilling)
            q = f"{query.strip()}{f' until:{until}' if until else ''}"
            params = {"q": q, "limit": page_size}
            if cursor:
                params["cursor"] = cursor

            r = retry(client.app.bsky.feed.search_posts, params)
            if not r or not getattr(r, "posts", None):
                break

            new_posts = r.posts
            rows.extend(new_posts)

            # Stop if we've hit our overall cap
            if len(rows) >= limit:
                break

            # Update cursor; if gone, set up next 'until:' backfill
            cursor = r.get("cursor")
            if cursor is None:
                until = new_posts[-1].indexed_at

            time.sleep(sleep_secs)

        # Dedupe & trim
        items = [item.model_dump() for item in rows]
        unique = deduplicate(items, "uri")
        return unique[:limit]


    def query(
        self,
        filter_term:  str,
        type_filter:  Optional[list]                 = None,
        has_link:     bool                           = False,
        max_items:    Optional[int]                  = None,
        cutoff_time:  Optional[Union[str, datetime]] = None,
        to_row:       bool                           = True,
        batch_size:   int                            = None,
        dump_kwargs:  dict                           = None
    ) -> Optional[list]:

        if type_filter:
            validate_type_filter(type_filter)

        if cutoff_time and not from_cli():
            cutoff_dt = normalize_cutoff_time(cutoff_time, 'historical')
        elif cutoff_time:
            cutoff_dt = cutoff_time

        results = []
        cursor  = None
        stop    = False

       limit   = min(100, max_items) if max_items else 100

        try:
            while not stop:
                resp = self._search_posts_page(filter_term, cursor, limit)

                for post in resp.posts:
                    post_data = post.model_dump()

                    if 'cutoff_dt' in locals():
                        created_dt = parser.isoparse(post_data["record"]["created_at"])
                        if created_dt <= cutoff_dt:
                            print(
                                f'\ncutoff_time ({cutoff_dt.isoformat()}) reached: stopping....'
                            )
                            stop = True
                            break

                    item = self._convert_post_to_item(post_data)
                    self._handle_item(item, to_row, dump_kwargs, results)

                    if max_items and (self.item_counter['count'] >= max_items):
                        print(f'\nmax_items ({max_items}) reached: stopping...')
                        stop = True

                if not resp.cursor:
                    

                 elif stop:
                    break

                cursor = resp.cursor
                time.sleep(0.25)

        except KeyboardInterrupt:
            print("\nInterrupted by user — returning collected results.")

        if not dump_kwargs:          # <- EXECUTED AS MODULE
            return results
