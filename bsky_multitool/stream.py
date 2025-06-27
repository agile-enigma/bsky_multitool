import base64
import csv
from datetime import datetime, timezone
from dateutil import parser
from functools import lru_cache, partial
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import (
    Any, Callable, Dict,
    List, Optional, Union
)

import atproto
from atproto_firehose.models import MessageFrame
from atproto_client.models.utils import get_or_create
from dotenv import load_dotenv

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
    str_to_re,
    structure_item,
    validate_type_filter,
)

# ──────────── fire-hose frame handler ──────────────────────────────────
def on_message(message, test_function, handler):
    message = atproto.parse_subscribe_repos_message(message)

    if isinstance(message, atproto.models.ComAtprotoSyncSubscribeRepos.Commit):
        blocks = atproto.CAR.from_bytes(message.blocks).blocks

        for op in message.ops:
            uri = atproto.AtUri.from_str(f'at://{message.repo}/{op.path}')
            raw = blocks.get(op.cid)
            if not raw:
                continue

            try:
                record      = get_or_create(raw, strict=False)
                record_dict = record.model_dump()
            except Exception as e:
                continue

            if record_dict is None or record_dict.get('py_type') is None:
                continue

            item = {
                'repo':        message.repo,       # DID of the user whose repo is being updated
                'revision':    message.rev,        # Current revision number of the user's repo
                'sequence':    message.seq,        # Global firehose sequence number (monotonic)
                'action':      op.action,          # Type of operation: 'create', 'update', or 'delete'
                
                'uri':         str(uri),           # Full AT URI for the record (e.g., at://did/collection/rkey)
                'cid':         str(op.cid),        # Content Identifier (hash of the record block)
                'path':        op.path,            # Path inside the repo (format: collection/rkey)
                'collection':  uri.collection,     # Collection type (e.g., app.bsky.feed.post)

                'record':      record_dict,        # Parsed content of the post (or like, follow, etc.)
                'action_type': get_type(           # Human-friendly classification: post, reply, quote, etc.
                                   uri.collection,
                                   op.action,
                                   record_dict
                                )
                }

            if test_function(item):
                handler(item)


# ──────────── live stream bootstrap ────────────────────────────────────
def monitor_bsky_firehose(test_function, handler):
    firehose = atproto.FirehoseSubscribeReposClient()

    try:
        firehose.start(
            lambda message: on_message(message, test_function, handler)
        )
    finally:
        # Make sure the socket / thread is closed
        try:
            firehose.stop()          # SDK ≥ 0.3
        except AttributeError:
            firehose.close()         # early SDKs


# # ──────────── FirehoseStreamer ───────────────────────────────────────
class firehoseStreamer:
    def __init__(
        self,
        handle:             Optional[str]            = None,
        app_password:       Optional[str]            = None,
        client:             Optional[atproto.Client] = None,
        get_author_data_fn: Optional[Callable]       = None,
        get_post_data_fn:   Optional[Callable]       = None
    ):
        self.client = get_client(handle, app_password, client)

        if get_author_data_fn is not None and get_post_data_fn is not None: # <- EXECUTED FROM CLI
            self.get_author_data_cached  = get_author_data_fn
            self.get_post_data_cached    = get_post_data_fn
        else:
            raw_get_author, raw_get_post = make_cached_fetchers(self.client)
            self.get_author_data_cached  = lru_cache(maxsize=1024)(raw_get_author)
            self.get_post_data_cached    = lru_cache(maxsize=1024)(raw_get_post)

        self.item_count = 0

    def _stop_stream(self):
        """
        Stop firehose depending on SDK version.
        """
        try:
            self.firehose.stop()
        except AttributeError:
            self.firehose.close()

    def _filter_item(self, item: dict) -> bool:
        return master_filter(
            item,
            filter_term = self.filter_term,
            type_filter = self.type_filter,
            has_link    = self.has_link
        )

    def _collect_handler(self, item: dict) -> None:
        item_structured = finalize_item_processing(
            item,
            self.get_author_data_cached,
            self.get_post_data_cached
        )
        
        if not self.to_row:
            self.results.append(item_structured)
        else:
            item_flatten_jsoned = flatten_json(item_structured)
            self.results.append(item_flatten_jsoned)

        print(f'{len(self.results)} items processed', end='\r', flush=True)

        if self.max_items and (len(self.results) >= self.max_items):
            print(f'\nmax_items ({self.max_items}) reached: streamer closing...')
            self._stop_stream()

        if (self.cutoff_time and 
            parser.isoparse(item['post_data']['created_at']) >= self.cutoff_time
        ):
            print(f'\ncutoff_time ({self.cutoff_time.isoformat()}) reached: streamer closing...')
            self._stop_stream()

    def start(
        self,
        filter_term: Optional[str]                    = None,
        type_filter: Optional[List[str]]              = None,
        has_link:    Optional[bool]                   = False,
        max_items:   Optional[int]                    = None,
        cutoff_time: Optional[Union[str, datetime]]   = None,
        to_row:      bool                             = False,
        sink:        Optional[Callable[[dict], None]] = None
    ) -> Optional[List[Dict[str, Any]]]:

        if filter_term and not isinstance(filter_term, re.Pattern):
            filter_term = str_to_re(filter_term)

        if type_filter:
            validate_type_filter(type_filter)

        if cutoff_time and not from_cli():
            cutoff_dt = normalize_cutoff_time(cutoff_time, 'stream')
        elif cutoff_time:
            cutoff_dt = cutoff_time

        self.filter_term = filter_term
        self.type_filter = type_filter
        self.has_link    = has_link
        self.max_items   = max_items
        self.cutoff_time = cutoff_dt if 'cutoff_dt' in locals() else None
        self.to_row      = to_row

        self.results  = []
        self.firehose = atproto.FirehoseSubscribeReposClient()

        # Define which handler to use:
        self.handler = sink if sink else self._collect_handler

        try:
            self.firehose.start(
                partial(
                    on_message,
                    test_function = self._filter_item,
                    handler       = self.handler
                )
            )
        except KeyboardInterrupt:
            print('Interrupted by user — returning collected results.')
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
        finally:
            self._stop_stream()

        # Only return results if running in internal collect mode
        if not sink:
            return self.results

