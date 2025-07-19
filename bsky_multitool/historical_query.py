from datetime import datetime
from dateutil import parser
from functools import lru_cache
import json
import os
from pathlib import Path
import random
import re
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Union


import atproto

from .utils import (
    _safe_json,
    dump_to_file,
    finalize_item_processing,
    flatten_json,
    get_author_data,
    get_client,
    get_hashtags,
    get_mentions,
    get_post_data,
    get_type,
    make_cached_fetchers,
    master_filter,
    normalize_timestamp,
    retry,
    validate_type_filter,
)


class historicalQuery:
    def __init__(
        self,
        handle:             Optional[str]            = None,
        app_password:       Optional[str]            = None,
        client:             Optional[atproto.Client] = None,
        get_author_data_fn: Optional[Callable]       = None,
        get_post_data_fn:   Optional[Callable]       = None,
        is_from_cli:        bool                     = False
    ):
        self.client      = get_client(handle, app_password, client)
        self.is_from_cli = is_from_cli

        if get_author_data_fn is not None and get_post_data_fn is not None: # <- EXECUTED FROM CLI
            self.get_author_data_cached  = get_author_data_fn
            self.get_post_data_cached    = get_post_data_fn
        else:
            raw_get_author, raw_get_post = make_cached_fetchers(self.client)
            self.get_author_data_cached  = lru_cache(maxsize=1024)(raw_get_author)
            self.get_post_data_cached    = lru_cache(maxsize=1024)(raw_get_post)

        self.item_counter = {'count': 0}


    def _retry(
        self,
        fn: Callable,
        params: Dict[str, Any],
        retries: int = 5,
        delay: float = 5.0,
        backoff: float = 2.0,
    ):
        """
        Retry fn(*args) with exponential backoff.
        - retries: max attempts
        - delay: initial wait
        - backoff: multiplier for each retry
        """
        attempt = 1
        while attempt <= retries:
            try:
                return fn(params)
            except Exception as e:
                if attempt == retries:
                    print(f"All {retries} retries failed: {e}")
                    raise
                sleep_time = delay * (backoff ** (attempt - 1))
                # add jitter of ±10%
                jitter = random.uniform(0.9, 1.1)  
                actual_sleep = sleep_time * jitter
                print(
                    f"Attempt {attempt}/{retries} failed ({e}); "
                    f" sleeping {actual_sleep:.1f}s…"
                )

                time.sleep(actual_sleep)
                attempt += 1


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

    def _handle_item(
        self,
        item:        dict,
        to_row:      bool,
        dump_kwargs: Optional[dict],
        results:     list
    ) -> None:

        if not self.is_from_cli:
            self.item_counter['count'] += 1
            item_structured = finalize_item_processing(
                item,
                self.get_author_data_cached,
                self.get_post_data_cached
            )
            if to_row:
                results.append(flatten_json(item_structured))
            else:
                results.append(item_structured)
            print(f'{self.item_counter['count']} items collected', end='\r', flush=True)
        
        else:
            dump_to_file(item, item_counter=self.item_counter, **dump_kwargs)


    def query(
        self,
        query_term:   str,
        type_filter:  Optional[List[str]]            = None,
        link_filter:  bool                           = False,
        max_items:    Optional[int]                  = None,
        since:        Optional[Union[str, datetime]] = None,
        until:        Optional[Union[str, datetime]] = None,
        to_row:       bool                           = True,
        dump_kwargs:  Optional[dict]                 = None
    ) -> Optional[list]:

        # ---- validate since ----
        since_formatted = None
        if since:
            try:
                since_formatted = normalize_timestamp(since, 'historical')
            except Exception as err:
                err_msg = f"Error parsing 'since': {err}"
                if self.is_from_cli:
                    print(f'{err_msg}\n')
                    sys.exit(1)
                # re-raise with same exception class
                raise err.__class__(err_msg).with_traceback(err.__traceback__)

        # ---- validate until ----
        until_formatted = None
        if until:
            try:
                until_formatted = normalize_timestamp(until, 'historical')
            except Exception as err:
                err_msg = f"Error parsing 'until': {err}"
                if self.is_from_cli:
                    print(f'{err_msg}\n')
                    sys.exit(1)
                raise err.__class__(err_msg).with_traceback(err.__traceback__)

        if since_formatted and until_formatted and since_formatted > until_formatted:
            err_msg = f"'since' ({since_formatted}) must be < 'until' ({until_formatted})"
            if self.is_from_cli:
                print(f'{err_msg}\n')
                sys.exit(1)
            raise ValueError(err_msg)

        if type_filter:
            try:
                validate_type_filter(type_filter, 'historical')
            except Exception as err:
                if self.is_from_cli:
                    print(f"{err}\n")
                    sys.exit(1)
                else:
                    raise

        cursor = None
        stop   = False
        limit  = min(100, max_items) if max_items else 100

        unique_uris = set()
        results = []

        try:
            # while not stop and (max_items is None or len(results) < max_items):
            stripped_qt = query_term.strip()
            while not stop:
                q = (
                    f"{stripped_qt}"
                    f"{f' since:{since_formatted}' if since_formatted else ''}"
                    f"{f' until:{until_formatted}' if until_formatted else ''}"
                )
                params = {"q": q, "limit": limit}
                if cursor:
                    params["cursor"] = cursor

                resp = self._retry(self.client.app.bsky.feed.search_posts, params)
                if not resp or not getattr(resp, "posts", None):
                    break

                for post in resp.posts:
                    post_data = post.model_dump()
                    uri = post_data["uri"]
                    if uri in unique_uris:
                        continue
                    unique_uris.add(uri)

                    item = self._convert_post_to_item(post_data)

                    if not master_filter(item, link_filter=link_filter, type_filter=type_filter):
                        continue
                    self._handle_item(item, to_row, dump_kwargs, results)

                    if max_items and (self.item_counter['count'] == max_items):
                        print(f'max_items ({max_items}) reached: stopping...\n')
                        stop = True
                        break

                if not stop:
                    # Update cursor; if gone, set up next 'until:' backfill
                    cursor = resp.cursor
                    if cursor is None:
                        until_formatted = item['record']['created_at']

                    if max_items and (max_items - self.item_counter['count']) < 100:
                        limit = max_items - self.item_counter['count']

                    time.sleep(1.0)

        except KeyboardInterrupt:
            print(
                f"\n\nInterrupted by user"
                f"{f' — returning collected results' if not self.is_from_cli else ''}.\n"
            )
        except Exception as e:
            print(
                f"\n\nUnexpected error occurred: {e}"
                f"{' Returning collected results.' if not self.is_from_cli else ''}\n"
            )
        if not self.is_from_cli:
            return results

