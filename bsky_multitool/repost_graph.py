import atproto
import numpy as np
import pandas as pd
import requests

from datetime import datetime
import json
import random
import time
from typing import Optional, Union
import warnings

from .utils import (
	get_client,
	normalize_timestamp
)


class repostGraph:
	def __init__(
		self,
		handle:             Optional[str]            = None,
		app_password:       Optional[str]            = None,
		client:             Optional[atproto.Client] = None,
		# get_author_data_fn: Optional[Callable]       = None,
		# get_post_data_fn:   Optional[Callable]       = None,
		is_from_cli:        bool                     = False
	):
		self.client      = get_client(handle, app_password, client)
		self.is_from_cli = is_from_cli

	def retry(self, method, params, retries=2):
		delay = 5
		while retries > 0:
			try:
				r = method(params)
				return r
			except Exception as e:
				print (f"error: {e}...sleeping " + str (delay) + "s")
				time.sleep (delay)
				delay = delay * 2
				retries = retries - 1

			return None


	def deduplicate(self, items, key):
	    keys = set ()
	    result = []
	    for item in items:
	        k = item[key]
	        if k not in keys:
	            result.append (item)
	            keys.add (k)

	    return result
	            
	    
	def loop_search_posts(self, params, client, max_items):
	    r = self.retry(self.client.app.bsky.feed.search_posts, params)
	    if r is None:
	        return None

	    cursor = r["cursor"]
	    rows   = []
	    rows.extend (r.posts)
	    until  = None
	    while cursor is not None and len (rows) < max_items:
	        time.sleep (1)
	        r = self.retry(
	        		self.client.app.bsky.feed.search_posts,
	        		{
	        			"q"      : re.sub(r'until:\S+', until, params['q']) if until else params['q'],
	        			"cursor" : cursor,
	        			"limit"  : min(max_items - len(rows), 100)
        			}
    			)
	        cursor = r["cursor"]
	        rows.extend(r.posts)
	        print(f'{len(rows)} items collected', end='\r', flush=True)

	        if cursor is None and len (rows) < limit:
	            t = " until:" + rows[-1]["indexed_at"]
	            if t != until:
	                until = t
	                r = self.retry(
	                		self.client.app.bsky.feed.search_posts, 
	                        {"q" : query + until, "limit" : 100}
                        )

	                cursor = r["cursor"]
	                rows.extend (r.posts)

	    return [item.model_dump () for item in self.deduplicate(rows, "uri")]


	def by_repost_count(self, row):
	    green = min (row["count"] // 5, 255)
	    pink  = hex (max (16, 255 - green))[2:]

	    return "#" + pink + hex (max (16, green))[2:] + pink


	def get_reposted_by(self, uri, client):
	    r = self.retry(self.client.app.bsky.feed.get_reposted_by,
	            {"uri" : uri, "limit" : 100})
	    if r is None:
	        return None
	    cursor = r["cursor"]
	    rows = []
	    rows.extend (r.reposted_by)

	    while cursor is not None:
	        time.sleep (1)
	        r = self.retry(self.client.app.bsky.feed.get_reposted_by,
	                {"uri" : uri, "cursor" : cursor, "limit" : 100})
	        cursor = r["cursor"]
	        rows.extend (r.reposted_by)

	    return [item.model_dump () for item in rows]


	def search_and_hydrate_reposts(
		self,
		params,
		client,
		max_items,
	    min_reposts
    ):
	    posts = self.loop_search_posts(params, client, max_items)
	    for post in posts:
	        uri = post["uri"]

	        if post["repost_count"] >= min_reposts:
	            try:
	                reposts = self.get_reposted_by(uri, client)
	                if reposts is not None and len (reposts) > 0:
	                    post["reposts"] = reposts
	            except:
	                print ("error fetching reposts for " + uri)

	    return posts


	def generate_edges(
		self,
		posts,
		min_reposts       = 10,
		normalize_handles = True,
	    node_colorizer    = None,
	    edge_colorizer    = None
    ):
	    rows = []
	    for post in posts:
	        if "reposts" in post and len(post["reposts"]) >= min_reposts:
	            uri = post["uri"]
	            orig = post["author"]
	            handle = orig["handle"]
	            did = orig["did"]
	            for repost in post["reposts"]:
	                rows.append({
	                    "uri"             : uri,
	                    "orig_did"        : did,
	                    "orig_handle"     : handle,
	                    "reposter_did"    : repost["did"],
	                    "reposter_handle" : repost["handle"]
	                })

	    edges           = pd.DataFrame(rows)
	    g               = edges.groupby(["orig_did", "orig_handle"])
	    nodes           = pd.DataFrame({"count" : g.size ()}).reset_index ()
	    nodes           = nodes[["orig_did", "orig_handle", "count"]]
	    nodes["radius"] = np.sqrt(nodes["count"])

	    if node_colorizer:
	        nodes["color"] = nodes.apply(node_colorizer, axis=1)

	    df = edges.merge(nodes, on=["orig_did", "orig_handle"])

	    if node_colorizer:
	        if edge_colorizer:
	            df["edge_color"] = df.apply(edge_colorizer, axis=1)
	        else:
	            df["edge_color"] = df["color"]

	    if normalize_handles:
	        dids = {}
	        for i, r in df.iterrows():
	            did = r["orig_did"]
	            if did not in dids:
	                dids[did] = r["orig_handle"]

	            did = r["reposter_did"]
	            if did not in dids:
	                dids[did] = r["reposter_handle"]

	        df["orig_handle"]     = df["orig_did"].apply(lambda i: dids[i])
	        df["reposter_handle"] = df["reposter_did"].apply(
	                lambda i: dids[i])

	    return df


	def generate_network_graph(
		self,
		query_term:   str,
		since:        Optional[Union[str, datetime]] = None,
		until:        Optional[Union[str, datetime]] = None,
		max_items:    Optional[int]                  = None,
		min_reposts:  Optional[int]					 = 10
		):

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

        # if type_filter:
        #     try:
        #         validate_type_filter(type_filter, 'historical')
        #     except Exception as err:
        #         if self.is_from_cli:
        #             print(f"{err}\n")
        #             sys.exit(1)
        #         else:
        #             raise

		limit  = min(100, max_items) if max_items else 100

		try:
			q = (
				f"{query_term.strip()}"
				f"{f' since:{since_formatted}' if since_formatted else ''}"
				f"{f' until:{until_formatted}' if until_formatted else ''}"
			)
			params = {"q": q, "limit": limit}

			results = self.search_and_hydrate_reposts(
				params       = params,
				client       = self.client,
				max_items    = max_items,
				min_reposts  = min_reposts
			)

			# print(results)
			df = self.generate_edges(results, node_colorizer=self.by_repost_count)
			return df

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






