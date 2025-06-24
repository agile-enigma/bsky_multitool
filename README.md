# bsky_multitool

**bsky_multitool** is a Python interface for collecting data from the AT Protocol underlying Bluesky. It can be used both as a command-line tool and as an importable module and is geared towards media analysis.

It supports two modes:
1. **stream**: Collect data in real-time as it is streamed through the AT Firehose
2. **historical**: Collect historical Bluesky data.

Output fields can be found at the bottom of this README.

## 🛠️ Installation

To install bsky_multitool, follow these steps:

1. `git clone https://github.com/agile-enigma/bsky_multitool.git`

2. `cd bsky_multitool`

3. `pip install -e .`

4. Optionally (but recommended for convenience), enter your full Bluesky handle (i.e. {handle}.bsky.social) and app password in the .env-template file included in the repo and rename the file to .env

bsky_multiool can now be used a command-line utility from anywhere in your directory structure or accessed as a module via `import bsky_multitool`.

It is recommended that installation and setup are executed within a virtual environment. Python virtual environments can be created via either Conda or Python's built-in venv module.
* **conda**: https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html
* **venv**: https://www.w3schools.com/python/python_virtualenv.asp

## Usage
Authenticating to the AT Protocol requires a Bluesky handle and an application password. App passwords can be obtained at https://bsky.app/settings/app-passwords.

### 💻 Command Line

Global command-line options:
* **--handle**: Your full Bluesky handle (not necessary if entered in the .env file)
* **--app-password**: Your Bluesky app password (not necessary if entered in the .env file)
* **-h/--help**: Print the help menu

Example: `bsky_multitool --help`

Stream mode command-line options:
* **--filter-term**: Term to filter stream events by for collection. Can take simple strings, regex strings, or re.Pattern objects (only as a module only).
* **--type**: Event types to filter stream events by for collection. Types include: 'post', 'quote', 'repost', 'reply', 'like', 'other'.
* **--has-link**: Limit collection to events featuring a link.
* **--max-items**: The maximum number of items to collect.
* **--cutoff-time**: Future time at which to terminate the stream. Format: YYYY-MM-DD HH:MM (UTC)
* **--batch-size**: Number of items to include in each output file. (only relevant when file-format is set to 'json')
* **--outdir**: The name of the directory where output will be saved. (defaults to 'bsky_stream')
* **--file-format**: Format of the output file. Options include: 'csv', 'json', and 'jsonl'.
* **--help**: Print stream mode help menu

Example: `bsky_multitool stream --filter-term '(?=.*\bisrael\b)(?=.*\biran\b)' --type post --type quote --cutoff-time '2025-06-25 18:30' --file-format csv`

Stream mode command-line options:
* **--filter-term**: Term to filter stream events by for collection. Can only take simple strings.
* **--type**: Event types to filter stream events by for collection. Types include: 'post', 'quote', 'repost', 'reply', 'like', 'other'.
* **--has-link**: Limit collection to events featuring a link.
* **--max-items**: The maximum number of items to collect.
* **--cutoff-time**: Past time at which to terminate the stream. Format: YYYY-MM-DD HH:MM (UTC)
* **--batch-size**: Number of items to include in each output file. (only relevant when file-format is set to 'json')
* **--outdir**: The name of the directory where output will be saved. (defaults to 'bsky_historical')
* **--file-format**: Format of the output file. Options include: 'csv', 'json', and 'jsonl'.
* **--help**: Print stream mode help menu

Example: `bsky_multitool historical --filter-term 'Gaza' --type quote --type reply --max-items 250 --has-link --file-format csv`

To run global options alongside a bsky_multitool mode, place them immediately prior to the mode designator:
`bsky_multitool --handle my_handle.bsky.social --app-password myapppassword historical --filter-term 'Gaza' --type quote --type reply --max-items 250 --has-link --file-format csv`

### 🧩 Importable Module

**Streamer**:
```python
import os
import re
import sys

from dotenv import load_dotenv
load_dotenv()

from bsky_multitool import FirehoseStreamer


handle = os.getenv('BSKY_HANDLE')
app_password = os.getenv('BSKY_APP_PSWD')

streamer = FirehoseStreamer(
    handle       = handle,
    app_password = app_password,
)

pattern = re.compile(r'(?=.*\bisrael\b)(?=.*\biran\b)', re.IGNORECASE)

results = streamer.start(
    filter_term  = pattern,             # optional
    type_filter  = ['quote', 'repost'], # optional (default: all types)
    has_link     = False,               # optional (default: False)
    max_items    = 10,                  # optional (default: no max_items)
    to_row       = True,                # optional (default: False)
    cutoff_time  = '2025-06-21 14:15'   # optional (default: no cutoff_time)
)
```

**HistoricalQuery**:
```python
import os
import re
import sys

from dotenv import load_dotenv
load_dotenv()

from bsky_streamer import HistoricalQuery


handle = os.getenv('BSKY_HANDLE')
app_password = os.getenv('BSKY_APP_PSWD')

hq = HistoricalQuery(
    handle       = handle,
    app_password = app_password,
)

response = hq.query(
    filter_term  = 'Chicago',           # <- optional
    type_filter  = ['quote', 'repost'], # <- optional (default all types)
    has_link     = False,               # <- optional (default False)
    max_items    = 10,                  # <- optional (default no max_items)
    to_row       = True,                # <- optional (default False)
    cutoff_time  = '2024-06-21 14:15'   # <- optional (default no cutoff_time)
)
```

## 📄 Output Fields

| Field                  | Description |
|------------------------|-------------|
| `did`                  | Decentralized Identifier of the post author |
| `author_handle`        | Bluesky username of the post author |
| `display_name`         | Display name set by the author |
| `avatar`               | URL of the author’s profile picture |
| `account_creation`     | Date/time the author's account was created |
| `description`          | Author’s bio/description |
| `verification`         | Verification info (if any) |
| `viewer`               | Metadata about the viewer’s relationship to the post |
| `followers_count`      | Number of followers the author has |
| `follows_count`        | Number of accounts the author follows |
| `posts_count`          | Total number of posts by the author |
| `pinned_post`          | URI of the author's pinned post (if any) |
| `author_feedgens`      | Feed generators associated with the author |
| `author_labeler`       | Labeler services associated with the author |
| `author_lists`         | Lists the author is part of |
| `author_starter_packs` | Starter packs linked to the author |
| `author_labels`        | Moderation labels applied to the author |
| `uri`                  | Unique resource identifier of the post |
| `collection`           | Type of post (e.g. post, repost, like) |
| `path`                 | Storage path used internally (e.g. collection/uri) |
| `cid`                  | Content ID of the post |
| `action`               | Type of action taken (e.g. create, delete) |
| `revision`             | Revision number of the post (if available) |
| `sequence`             | Ordering value from the firehose (if applicable) |
| `action_type`          | Interpreted type of post (e.g. original, reply, repost) |
| `post_url`             | Public URL of the post |
| `text`                 | Text content of the post |
| `timestamp`            | Creation time of the post |
| `indexed_at`           | Time when the post was indexed by the network |
| `py_type`              | Internal type used for parsing |
| `langs`                | Language codes detected in the post |
| `hashtags`             | Hashtags used in the post |
| `embedded_urls`        | URLs embedded in the post |
| `mentions`             | Users mentioned in the post |
| `reply_count`          | Number of replies to the post |
| `repost_count`         | Number of reposts |
| `quote_count`          | Number of quote-posts |
| `like_count`           | Number of likes |
| `facets`               | Rich-text metadata (e.g. links, mentions) |
| `entities`             | Structured metadata (e.g. links, mentions) |
| `labels`               | Moderation labels applied to the post |
| `embed`                | Embedded media or post (if any) |
| `target_did`           | DID of the target user (for replies/likes/etc.) |
| `target_handle`        | Handle of the target user |
| `target_display_name`  | Display name of the target user |
| `target_data_text`     | Text from the target post (e.g. in replies/quotes) |
