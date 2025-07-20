import csv
from dateutil import parser
import os
import time
import json
from functools import lru_cache
import re
from pathlib import Path
import sys

import click
import atproto
from atproto_client.exceptions import UnauthorizedError
from dotenv import load_dotenv

# Import package functions and classes
from .stream import firehoseStreamer
from .historical_query import historicalQuery
from .graph_client import graphClient

from .utils import (
    _safe_json,
    dump_to_file,
    finalize_item_processing,
    has_term,
    make_cached_fetchers,
    master_filter,
    normalize_timestamp
)

# Load .env automatically (from project root)
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--handle",       envvar="BSKY_HANDLE",   required=True, help="Bluesky handle (loaded from envvar BSKY_HANDLE).")
@click.option("--app-password", envvar="BSKY_APP_PSWD", required=True, help="App password (from envvar BSKY_APP_PSWD).")
@click.pass_context
def cli(ctx, handle, app_password):
    client = atproto.Client()
    try:
        client.login(handle, app_password)
    except UnauthorizedError as e:
        print('\nInvalid identifier or password. exiting...\n')
        sys.exit(1)

    raw_get_author, raw_get_post = make_cached_fetchers(client)
    get_author = lru_cache(maxsize=1024)(raw_get_author)
    get_post   = lru_cache(maxsize=1024)(raw_get_post)

    ctx.obj = {
        "handle":          handle,
        "client":          client,
        "get_author_data": get_author,
        "get_post_data":   get_post
    }


#---------------------------***STREAM MODE INTERFACE***------------------------------
@cli.command()
@click.option("--filter-term", default=None, help="Filter term or regex.")
@click.option("--type", "type_", multiple=True, default=None, 
              type=click.Choice(["post", "quote", "repost", "reply", "like", "other"]),
              help="Filter activity type.")
@click.option("--link-filter", is_flag=True, help="Only include posts containing links.")
@click.option("--max-items", type=int, required=False, help="Max number of items to collect.")
@click.option("--cutoff-time", required=False, help="Cutoff time in YYYY-MM-DD HH:MM format (UTC).")
@click.option("--batch-size", type=int, default=50, show_default=True)
@click.option("--out-dir", default="bsky_stream", show_default=True)
@click.option(
    "--file-format",
    type=click.Choice(["json", "jsonl", "csv"], case_sensitive=False),
    default="json",
    show_default=True,
    help="Choose output file format."
)

@click.pass_context
def stream(
    ctx, filter_term, type_, link_filter, batch_size,
    out_dir, file_format, max_items=None, cutoff_time=None
):
    """
    Start live firehose stream
    """
    streamer = firehoseStreamer(
        client             = ctx.obj["client"],
        get_author_data_fn = ctx.obj["get_author_data"],
        get_post_data_fn   = ctx.obj["get_post_data"],
        is_from_cli        = True
    )

    # Initialize output directory
    timestamp     = time.strftime('%Y%m%d_%H%M%S')
    base_filename = f"bsky_stream_{timestamp}"
    outdir_path   = Path(out_dir)
    outdir_path.mkdir(parents=True, exist_ok=True)

    # Initialize queue & counters
    queue = []
    item_counter = {'count': 0}

    dump_kwargs = dict(
        queue              = queue,
        outdir_path        = outdir_path,
        base_filename      = base_filename,
        get_author_data_fn = ctx.obj["get_author_data"],
        get_post_data_fn   = ctx.obj["get_post_data"],
        file_format        = file_format,
        batch_size         = batch_size,
        item_counter       = item_counter
    )

    # Wrapped sink that adds counting + auto firehose stopping
    def sink_with_count(item: dict) -> None:
        dump_to_file(item, **dump_kwargs)

        if streamer.max_items and (item_counter['count'] >= streamer.max_items):
            print(f'\nmax_items ({streamer.max_items}) reached: streamer closing...')
            streamer._stop_stream()

        if (streamer.cutoff_time and
            parser.isoparse(item['post_data']['created_at']) >= streamer.cutoff_time
        ):
            print(
                f'\ncut_off_time ({streamer.cutoff_time.isoformat()}) reached: streamer closing...'
            )
            streamer._stop_stream()

    click.echo(f"Starting live stream for @{ctx.obj["handle"]}…\n")

    type_ = list(type_) if type_ else None
    streamer.validation_error = False
    streamer.start(
        filter_term = filter_term,
        type_filter = type_,
        link_filter = link_filter,
        max_items   = max_items,
        cutoff_time = cutoff_time,
        to_row      = False,
        sink        = sink_with_count
    )

    if file_format == 'json' and not streamer.validation_error:
        print('Performing final flush...', end='\n\n')
        dump_to_file(None, **dump_kwargs, final_flush=True)


#--------------------------***HISTORICAL MODE INTERFACE***-----------------------------
@cli.command()
@click.option("--query-term", required=True, help="Search query string.")
@click.option("--since", required=False, help="'YYYY-MM-DD HH:MM' format (UTC).")
@click.option("--until", required=False, help="'YYYY-MM-DD HH:MM' format (UTC).")
@click.option("--from", "from_user", help="Confine results to posts from a specific user (do not include @).")
@click.option("--to",   "to_user",   help="Confine results to posts mentioning a specific user (do not include @).")
@click.option("--lang",              help="Confine results to posts from a specific language (ISO 639-1).")
@click.option("--domain",
                help="Confine results to posts linking to a specific domain (including subdomains and URL paths)."
             )
@click.option("--type", "type_", multiple=True, default=None, 
              type=click.Choice(["post", "quote", "reply"]),
              help="Filter activity type.")
@click.option("--link-filter", is_flag=True, help="Only include posts containing links.")
@click.option("--max-items", type=int, required=False, help="Max number of items to collect.")
@click.option("--batch-size", type=int, default=50, show_default=True)
@click.option("--out-dir", default="bsky_historical", show_default=True)
@click.option(
    "--file-format",
    type=click.Choice(["json", "jsonl", "csv"], case_sensitive=False),
    default="json",
    show_default=True,
    help="Choose output file format."
)
@click.pass_context
def historical(
    ctx, query_term, since, until, from_user, to_user,
    lang, domain, type_, link_filter, max_items,
    batch_size, out_dir, file_format
):
    """
    Perform historical search query
    """

    hquery = historicalQuery(
        client             = ctx.obj["client"],
        get_author_data_fn = ctx.obj["get_author_data"],
        get_post_data_fn   = ctx.obj["get_post_data"],
        is_from_cli        = True
    )

    timestamp     = time.strftime('%Y%m%d_%H%M%S')
    base_filename = f"bsky_stream_{timestamp}"
    path          = Path(out_dir)
    path.mkdir(parents=True, exist_ok=True)

    queue = []

    dump_kwargs = dict(
        queue              = queue,
        outdir_path        = path,
        base_filename      = base_filename,
        # client             = hquery.client,
        get_author_data_fn = ctx.obj["get_author_data"],
        get_post_data_fn   = ctx.obj["get_post_data"],
        file_format        = file_format,
        batch_size         = batch_size
    )

    click.echo(f"\nStarting historical query for @{ctx.obj["handle"]}…\n")

    hquery.query(
        query_term  = query_term,
        since       = since,
        until       = until,
        from_user   = from_user,
        to_user     = to_user,
        lang        = lang,
        domain      = domain,
        type_filter = list(type_) if type_ else None,
        link_filter = link_filter,
        max_items   = max_items,
        to_row      = False,           # <- NOT APPLICABLE IN CLI MODE
        dump_kwargs = dump_kwargs
    )

    if file_format == 'json':
        print('Performing final flush...', end='\n\n')
        dump_to_file(None, **dump_kwargs, final_flush=True)


#--------------------------***GET FOLLOWERS INTERFACE***-----------------------------
@cli.command()
@click.option("--did-or-handle", required=True, help="DID or handle of the account you want the followers of.")
@click.option("--out-dir", default="bsky_followers", show_default=True)
@click.option(
    "--file-format",
    type=click.Choice(["json", "jsonl", "csv"], case_sensitive=False),
    default="json",
    show_default=True,
    help="Choose output file format."
)
@click.pass_context
def followers(ctx, did_or_handle, out_dir, file_format):
    graph_client = graphClient(client= ctx.obj["client"])
    followers    = graph_client.get_followers(did_or_handle)
    if not followers:
        print(f'No followers found for {did_or_handle}.')
        sys.exit(0)

    outdir_path   = Path(out_dir)
    outdir_path.mkdir(parents=True, exist_ok=True)

    timestamp = time.strftime('%Y%m%d_%H%M%S')
    filename = f"{did_or_handle}_followers_{timestamp}"

    file_path = outdir_path / f'{filename}.json'

    if file_format   == 'json':
        with file_path.open('w') as f:
            json.dump(followers, f, default=_safe_json, indent=2)
    elif file_format == 'jsonl':
        file_path = file_path.with_suffix('.jsonl')
        with file_path.open('a') as f:
            for follower in followers:
                f.write(json.dumps(follower, default=_safe_json) + '\n')
    elif file_format == 'csv':
        file_path = file_path.with_suffix('.csv')
        with file_path.open('a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=followers[0].keys())
            writer.writeheader()
            writer.writerows(followers)


#--------------------------***GET FOLLOWING INTERFACE***-----------------------------
@cli.command()
@click.option("--did-or-handle", required=True, help="DID or handle of the account you want the followers of.")
@click.option("--out-dir", default="bsky_following", show_default=True)
@click.option(
    "--file-format",
    type=click.Choice(["json", "jsonl", "csv"], case_sensitive=False),
    default="json",
    show_default=True,
    help="Choose output file format."
)
@click.pass_context
def following(ctx, did_or_handle, out_dir, file_format):
    graph_client = graphClient(client= ctx.obj["client"])
    following    = graph_client.get_following(did_or_handle)
    if not following:
        print(f'No accounts followed by {did_or_handle}.')
        sys.exit(0)

    outdir_path   = Path(out_dir)
    outdir_path.mkdir(parents=True, exist_ok=True)

    timestamp = time.strftime('%Y%m%d_%H%M%S')
    filename = f"{did_or_handle}_following_{timestamp}"

    file_path = outdir_path / f'{filename}.json'

    if file_format   == 'json':
        with file_path.open('w') as f:
            json.dump(following, f, default=_safe_json, indent=2)
    elif file_format == 'jsonl':
        file_path = file_path.with_suffix('.jsonl')
        with file_path.open('a') as f:
            for followed in following:
                f.write(json.dumps(followed, default=_safe_json) + '\n')
    elif file_format == 'csv':
        file_path = file_path.with_suffix('.csv')
        with file_path.open('a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=following[0].keys())
            writer.writeheader()
            writer.writerows(following)


if __name__ == "__main__":
    cli()