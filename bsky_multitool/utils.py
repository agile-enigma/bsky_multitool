import __main__
import base64
import csv
from datetime import datetime, timezone
from dateutil import parser
import json
from typing import Any, Callable, Optional, Union
import os
from pathlib import Path
import re
import time

import atproto
from atproto_client.exceptions import UnauthorizedError

RUNNING_AS_SCRIPT = (__name__ == "__main__")


# ──────────── validation ──────────────────────────────────────────────────
def from_cli() -> bool:
    """
    True when *this* file is the process entry point, False when imported.
    """
    return RUNNING_AS_SCRIPT


def str_to_re(filter_term: str) -> str:
    try:
        filter_term = re.compile(filter_term, re.IGNORECASE)
    except re.error as e:
        raise ValueError(f"Invalid regex pattern: {e}")

    return filter_term


def normalize_cutoff_time(
    cutoff: Union[str, datetime],
    mode:   str
) -> Optional[datetime]:
    """
    Accepts a str, datetime, or None.
    Returns a timezone-aware datetime in UTC, or None if no cutoff supplied.
    Raises:
        ValueError / TypeError with clear message when input is invalid.
    """
    # If a string → parse
    if isinstance(cutoff, str):
        try:
            # Will parse either `YYYY-MM-DD` or full ISO e.g. `YYYY-MM-DD HH:MM`
            # Handles 'Z' and timezone offsets, too.
            cutoff_dt = parser.isoparse(cutoff)
        except (ValueError, parser.ParserError):
            raise ValueError(
                "cutoff_time string must be 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM' "
                "optionally with timezone (e.g. 'Z' or '+02:00')."
            )
    # If already a datetime
    elif isinstance(cutoff, datetime):
        cutoff_dt = cutoff
    else:
        raise TypeError("cutoff_time must be str, or datetime")

    # Make timezone-aware (assume UTC if naïve)
    if cutoff_dt.tzinfo is None:
        cutoff_dt = cutoff_dt.replace(tzinfo=timezone.utc)
    else:
        cutoff_dt = cutoff_dt.astimezone(timezone.utc)

    # Ensure cutoff_time is mode-compatible
    if mode == 'stream':
        if cutoff_dt <= datetime.now(timezone.utc):
            raise ValueError(
                "\nValueError: cutoff_time in stream mode must be in the future (UTC)\n"
            )
    elif mode == 'historical':
        if cutoff_dt >= datetime.now(timezone.utc):
            raise ValueError(
                "\nValueError: cutoff_time in historical mode must be in the past (UTC)\n"
            )

    return cutoff_dt


def validate_type_filter(type_filter: list[str]) -> None:
    if type_filter:
        if not isinstance(type_filter, list):
            raise ValueError(
                "`type_filter` must be a list of strings. Valid types "
                "include: 'post', 'repost', 'quote', 'reply', and 'like'."
            )
        valid_types = {"post","quote","repost","reply","like","other"}
        invalid = [t for t in type_filter if t not in valid_types]
        if invalid:
            raise ValueError(
                f"Invalid type(s) entered: {invalid}. Valid types include: "
                "'post', 'repost', 'quote', 'reply', and 'like'."
            )


def get_client(
    handle:       Optional[str],
    app_password: Optional[str],
    client:       Optional[atproto.Client]
) -> atproto.Client:
    if not client:
        if not handle or not app_password:
            raise ValueError(
                "\nBoth handle and app_password must be provided as arguments "
                "or set in environment variables. You may alternatively pass a client.\n"
            )
        client = atproto.Client()
        try:
            client.login(handle, app_password)
        except UnauthorizedError as e:
            raise UnauthorizedError(
                f"\nInvalid identifier or password.\n"
            )

    return client


# ──────────── miscellaneous helpers ────────────────────────────────────────────────
def retry(method, params):
    retries = 5
    delay = 1
    while retries > 0:
        try:
            response = method(params)
            return response
        except Exception as e:
            if 'AccountTakedown' in str(e):
                print(f'    Fatal error: {e} – will not retry.')
                break
            print(f'    {e} – sleeping {delay}s')
            time.sleep(delay)
            delay *= 2
            retries -= 1

    return None


def _safe_json(obj):
    '''Fallback for json.dump: encode bytes → base64 str, else str().'''
    if isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(obj).decode()

    return str(obj)          # last-resort fallback


def make_cached_fetchers(client):
    profile_cache = {}
    post_cache = {}

    def get_author_data_cached(did: str):
        if did not in profile_cache:
            profile_cache[did] = get_author_data(did, client)
        return profile_cache[did]

    def get_post_data_cached(uri: str):
        if uri not in post_cache:
            post_cache[uri] = get_post_data(uri, client)
        return post_cache[uri]

    return get_author_data_cached, get_post_data_cached


# ──────────── profile batching ─────────────────────────────────────────
def get_target_data(
    item: dict,
    get_post_data_fn,
    get_author_data_fn
) -> dict | None:
    target_uri = None

    if item['action_type'] == 'repost':
        target_uri = item['record'].get('subject', {}).get('uri')

    elif item['action_type'] == 'quote':
        embed = item['record'].get('embed') or {}
        etype = embed.get('py_type', '')
        if etype == 'app.bsky.embed.recordWithMedia':
            target_uri = embed.get('record', {}).get('record', {}).get('uri')
        elif etype == 'app.bsky.embed.record':
            target_uri = embed.get('record', {}).get('uri')

    elif item['action_type'] == 'reply':
        target_uri = item['record'].get('reply', {}).get('root', {}).get('uri')

    elif item['action_type'] == 'like':
        target_uri = item['record'].get('subject', {}).get('uri')

    if not target_uri:
        print(f"⚠ Warning: No target URI found for item with action_type '{item['action_type']}'")
        return None

    try:
        post_data = get_post_data_fn(target_uri)
        if not post_data:
            return None

        post_data = post_data.get('record', {})

        match_did      = re.search(r'(did:[^/]+)', target_uri) # <- extract DID from target_uri
        target_did     = match_did.group(1) if match_did else None
        author_data = get_author_data_fn(target_did) if target_did else {}

        post_data.update({
            'uri': target_uri,
            'post_url': get_post_url(author_data.get('handle'), target_uri)
        })

        return {
            'post_data':   post_data,
            'author_data':  author_data
        }

    except Exception as e:
        print(f'get_posts failed for {target_uri}: {e}')
        return None


# ──────────── fetch profile ────────────────────────────────────────────
def flatten_json(item: dict) -> dict:
    author_data = item.get('author_data', {})
    post_data = item.get('post_data', {})
    target_data = item.get('target_data', {})

    record = post_data.get('record', {})
    associated = author_data.get('associated', {})

    return {
        # ── Identity (User-Level) ───────────────────────────────
        'did':                  author_data.get('did'),               # User's data repository (linked to DID)
        'author_handle':        author_data.get('handle'),            # @handle of the user
        'display_name':         author_data.get('display_name'),      # User's display name (profile name)
        'avatar':               author_data.get('avatar'),            # URL to profile picture
        'account_creation':     author_data.get('created_at'),        # When the account was created
        'description':          author_data.get('description'),       # Bio / about section
        'verification':         author_data.get('verification'),      # Verification status and details
        'viewer':               author_data.get('viewer'),            # Viewer relationship metadata

        # ── Social Metadata ─────────────────────────────────────
        'followers_count':      author_data.get('followers_count'),   # Total followers
        'follows_count':        author_data.get('follows_count'),     # Total following
        'posts_count':          author_data.get('posts_count'),       # Total posts
        'pinned_post':          author_data.get('pinned_post'),       # Pinned post (if any)

        # ── Associated Features ─────────────────────────────────
        'author_feedgens':      associated.get('feedgens'),           # Feed generators
        'author_labeler':       associated.get('labeler'),            # Labeler role
        'author_lists':         associated.get('lists'),              # Lists authored or part of
        'author_starter_packs': associated.get('starter_packs'),      # Starter pack participation
        'author_labels':        author_data.get('labels'),            # Any content/user labels

        # ── Record Identity & Location ──────────────────────────
        'uri':                  post_data.get('uri'),                 # Full AT URI **PROBLEM**
        'collection':           post_data.get('collection'),          # Collection type
        'path':                 post_data.get('path'),                # Repo path
        'cid':                  post_data.get('cid'),                 # CID (content ID hash)

        # ── Stream Metadata ─────────────────────────────────────
        'action':               post_data.get('action'),              # Firehose action
        'revision':             item.get('revision'),                 # Repo revision
        'sequence':             item.get('sequence'),                 # Firehose sequence number
        'action_type':          post_data.get('action_type'),         # Custom type: post, quote, etc.

        # ── Post Data ───────────────────────────────────────────
        'post_url':             post_data.get('post_url'),            # URL of the post
        'text':                 record.get('text'),                   # Main body of the post
        'created_at':           post_data.get('created_at'),          # When post was created
        'indexed_at':           post_data.get('indexed_at'),          # When it was indexed
        'py_type':              record.get('py_type'),                # Post record schema type
        'langs':                record.get('langs'),                  # Detected languages
        'hashtags':             post_data.get('hashtags'),            # Hashtags/tags
        'embedded_urls':        post_data.get('embedded_urls'),       # Any extracted/embedded URLs
        'mentions':             post_data.get('mentions'),
        
        # ── Engagement Metrics ──────────────────────────────────
        'reply_count':          post_data.get('reply_count'),         # Replies
        'repost_count':         post_data.get('repost_count'),        # Reposts
        'quote_count':          post_data.get('quote_count'),         # Quotes
        'like_count':           post_data.get('like_count'),          # Likes

        # ── Rich Text / Embeds ──────────────────────────────────
        'facets':               record.get('facets'),                 # Rich text facets
        'entities':             record.get('entities'),               # Extracted entities
        'labels':               record.get('labels'),                 # Moderation/content labels
        'embed':                record.get('embed'),                  # Media embeds

        # ── Target Post Data ─────────────────────────────────────
        'target_did': (
            target_data.get('author_data', {}).get('did')          # Target post author DID
            if target_data else None
        ),
        'target_handle': (
            target_data.get('author_data', {}).get('handle')       # Target post author handle
            if target_data else None
        ),
        'target_display_name': (
            target_data.get('author_data', {}).get('display_name') # Target post author display name
            if target_data else None
        ),
        'target_post_uri': (
            target_data.get('post_data', {}).get('uri')            # Target post URI
            if target_data else None
        ),
        'target_post_url': (
            target_data.get('post_data', {}).get('post_url')       # Target post url
            if target_data else None
        ),
        'target_post_text': (
            target_data.get('post_data', {}).get('text')           # Target post text
            if target_data else None
        )
    }


def get_author_data(did: str, client: atproto.Client):
    r = retry(client.app.bsky.actor.get_profile, {'actor': did})

    return r.model_dump()


def get_post_data(uri: str, client: atproto.Client) -> dict:
    response = client.app.bsky.feed.get_posts({'uris': [uri]})
    if response.posts:
        post_data = response.posts[0].model_dump()
        return response.posts[0].model_dump()

    return {}


def get_post_url(handle: str, uri: str) -> str:
    post_base = f"bsky.app/profile/{handle}/post/"
    rkey = re.search(r'at://[^/]+/[^/]+/([^/]+)', uri).group(1)

    return f'{post_base}{rkey}'


def get_hashtags(post_data: dict) -> list[str]:
    facets = (post_data.get('record', {}) or {}).get('facets') or []

    return [
        feature['tag']
        for facet in facets
        for feature in facet.get('features', [])
        if 'tag' in feature
    ]


def get_mentions(post_data: dict) -> list[str]:
    facets = (post_data.get('record', {}) or {}).get('facets') or []

    return [
        feature['did']
        for facet in facets
        for feature in facet.get('features', [])
        if 'did' in feature
    ]


def structure_item(item: dict, author_data: dict, post_data: dict) -> dict:
    # Assign nested data
    item['author_data'] = author_data

    if 'post_data' not in item: # <- for items coming in through bsky_streamer
        item['post_data'] = post_data

    post_url = get_post_url(author_data['handle'], post_data.get('uri'))

    # Promote selected item fields into post_data
    item['post_data'].update({
        'created_at':    item['record'].get('created_at'),
        'post_url':      post_url,
        'embedded_urls': item.get('embedded_urls'),
        'mentions':      item.get('mentions'),
        'hashtags':      item.get('hashtags'),
        'action_type':   item.get('action_type'),
        'collection':    item.get('collection'),
        'path':          item.get('path'),
        'action':        item.get('action')
    })

    # Fields to remove from top-level item and nested post_data
    for key in [
        'repo', 'cid', 'uri', 'embedded_urls', 'action_type',
        'collection', 'path', 'action', 'record', 'mentions', 'hashtags'
    ]:
        item.pop(key, None)  # use pop(..., None) to avoid KeyErrors

    item['post_data'].pop('author', None)  # remove author from post_data if present

    return item


def finalize_item_processing(
    item: dict,
    get_author_data_fn,
    get_post_data_fn
) -> dict:
    author_data = get_author_data_fn(item['repo']) # <- More detailed author data than provided in post_data
    post_data   = get_post_data_fn(item['uri'])    # <- More detailed post data than provided in record_dict

    item['hashtags'] = get_hashtags(post_data)
    item['mentions'] = get_mentions(post_data)

    if item['action_type'] in ('quote', 'repost', 'reply', 'like'):
        item['target_data'] = get_target_data(item, get_post_data_fn, get_author_data_fn)
    else:
        item['target_data'] = None

    item_structured = structure_item(item, author_data, post_data)

    return item_structured


# ──────────── sink: batch → json files ─────────────────────────────────
def dump_to_file(
    item: Optional[dict[str, Any]],
    queue:              list,
    outdir_path:        Path,
    base_filename:      str,
    client:             atproto.Client,
    get_author_data_fn: Callable,
    get_post_data_fn:   Callable,
    file_format:        str,
    batch_size:         int  = 20,
    item_counter:       dict = None,
    final_flush:        bool = False
) -> None:
    if item_counter is None:
        item_counter = {'count': 0}

    if item:
        item_structured = finalize_item_processing(
            item,
            get_author_data_fn,
            get_post_data_fn
        )

    if file_format == 'json':
        if item:
            queue.append(item_structured)
            item_counter['count'] += 1

        if len(queue) >= batch_size or (final_flush and len(queue)): # <- POTENTIAL PROBLEM
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            filename = f"bsky_stream_{timestamp}_batch"
            micros = int((time.time() % 60) * 1_000_000)
            json_path = outdir_path / f'{filename}_{micros}.json'

            with json_path.open('w') as f:
                json.dump(queue, f, default=_safe_json, indent=2)

            print(f'{len(queue)} items written to {json_path}', end='\r', flush=True)

            queue.clear()

    elif file_format == 'jsonl':
        jsonl_path = outdir_path / f'{base_filename}.jsonl'

        with jsonl_path.open('a') as f:
            f.write(json.dumps(item_structured, default=_safe_json) + '\n')

        item_counter['count'] += 1
        print(f'{item_counter['count']} items written to {jsonl_path}', end='\r', flush=True)

    elif file_format == 'csv':
        item_row = flatten_json(item_structured)
        csv_path = outdir_path / f'{base_filename}.csv'
        write_header = not csv_path.exists() or csv_path.stat().st_size == 0

        with csv_path.open('a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=item_row.keys())
            if write_header:
                writer.writeheader()
            writer.writerow(item_row)

        item_counter['count'] += 1
        print(f"{item_counter['count']} items written to {csv_path}", end='\r', flush=True)


# ──────────── filters ───────────────────────────────────────────────────
def has_link_(item: dict) -> bool:                   # <-- HELPER
    urls = set()
    if item['collection'] == 'app.bsky.feed.post' and item['action'] == 'create':
        record = item['record']

        # external embed
        embed = record.get('embed', None)
        if embed and embed.get('external', None) and \
        embed['external'].get('uri', '').startswith('http'):
            urls.add(embed['external']['uri'])

        # facets
        for facet in record.get('facets', []) or []:
            for feature in facet.get('features', []) or []:
                uri = feature.get('uri', '')
                if uri.startswith('http'):
                    urls.add(uri)

    if not urls:
        return False
    item['embedded_urls'] = list(urls)

    return True


def has_term(item: dict, pattern: Union[str, re.Pattern]) -> bool:
    '''Match text in post content using regex pattern.'''
    text = item['record'].get('text', '')
    
    return bool(pattern.search(text))


def master_filter(
    item:        dict,
    filter_term: Optional[Union[str, re.Pattern]] = None,
    type_filter: Optional[list] = None,
    has_link:    bool = None
    ) -> bool:
    if type_filter is not None and item['action_type'] not in type_filter:
        return False

    if filter_term is not None:
        if not has_term(item, filter_term):
            return False

    if has_link is not None:
        if not has_link_(item):
            return False
        
    return True


# ──────────── fetch type ────────────────────────────────────────────────
def get_type(collection: str, action: str, record: dict) -> str:
    # All cases you care about are 'create'
    if action != 'create':
        return 'other'

    # Posts / quotes / replies 
    if collection == 'app.bsky.feed.post':
        embed = record.get('embed') or {}
        embed_type = embed.get('py_type', '')

        if embed_type in (
            'app.bsky.embed.record',
            'app.bsky.embed.recordWithMedia'):
            return 'quote'

        # reply is a dict when present, None / missing otherwise
        if record.get('reply'):
            return 'reply'

        return 'post'

    # Simple reposts
    if collection == 'app.bsky.feed.repost':
        return 'repost'

    # Likes
    if collection == 'app.bsky.feed.like':
        return 'like'

    return 'other'
