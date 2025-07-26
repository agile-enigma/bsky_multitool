from atproto import Client
import json
import numpy as np
import pandas as pd
import random
import requests
import time
import warnings


class repost_graph:
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

        self.item_counter = {'count': 0}

	def retry (method, params, retries=2):
	    delay = 5
	    while retries > 0:
	        try:
	            r = method (params)
	            return r
	        except Exception as e:
	            print (f"error: {e}...sleeping " + str (delay) + "s")
	            time.sleep (delay)
	            delay = delay * 2
	            retries = retries - 1
	    return None


	def deduplicate (items, key):
	    keys = set ()
	    result = []
	    for item in items:
	        k = item[key]
	        if k not in keys:
	            result.append (item)
	            keys.add (k)
	    return result
	            
	    
	def loop_search_posts (query, client, limit):
	    r = retry (client.app.bsky.feed.search_posts,
	            {"q" : query, "limit" : 100})
	    if r is None:
	        return None
	    cursor = r["cursor"]
	    rows = []
	    rows.extend (r.posts)
	    until = None
	    while cursor is not None and len (rows) < limit:
	        time.sleep (1)
	        r = retry (client.app.bsky.feed.search_posts,
	                {"q" : query + until if until else query,
	                 "cursor" : cursor, "limit" : 100})
	        cursor = r["cursor"]
	        rows.extend (r.posts)
	        print(f'{len(rows)} items collected', end='\r', flush=True)
	        if cursor is None and len (rows) < limit:
	            t = " until:" + rows[-1]["indexed_at"]
	            print (t)
	            if t != until:
	                until = t
	                print (until)
	                r = retry (client.app.bsky.feed.search_posts, 
	                        {"q" : query + until, "limit" : 100})
	                cursor = r["cursor"]
	                rows.extend (r.posts)
	    return [item.model_dump () for item in deduplicate (rows, "uri")]


	def get_reposted_by (uri, client, limit=50000):
	    r = retry (client.app.bsky.feed.get_reposted_by,
	            {"uri" : uri, "limit" : 100})
	    if r is None:
	        return None
	    cursor = r["cursor"]
	    rows = []
	    rows.extend (r.reposted_by)
	    while cursor is not None and len (rows) < limit:
	        time.sleep (1)
	        r = retry (client.app.bsky.feed.get_reposted_by,
	                {"uri" : uri, "cursor" : cursor, "limit" : 100})
	        cursor = r["cursor"]
	        rows.extend (r.reposted_by)
	    return [item.model_dump () for item in rows]


	def search_and_hydrate_reposts (query, client, item_counter,
	        min_reposts=10, limit=3000):
	    posts = loop_search_posts (query, client, limit=limit)
	    for post in posts:
	        uri = post["uri"]
	        if post["repost_count"] >= min_reposts:
	            try:
	                reposts = get_reposted_by (uri, client)
	                if reposts is not None and len (reposts) > 0:
	                    # item_counter['count'] += 1
	                    # print(f'{item_counter['count']} items collected', end='\r', flush=True)
	                    post["reposts"] = reposts
	            except:
	                print ("error fetching reposts for " + uri)
	    return posts

	def generate_edges (posts, min_reposts=10, normalize_handles=True,
	                    node_colorizer=None, edge_colorizer=None):
	    rows = []
	    for post in posts:
	        if "reposts" in post and len (post["reposts"]) >= min_reposts:
	            uri = post["uri"]
	            orig = post["author"]
	            handle = orig["handle"]
	            did = orig["did"]
	            for repost in post["reposts"]:
	                rows.append ({
	                    "uri" : uri,
	                    "orig_did"        : did,
	                    "orig_handle"     : handle,
	                    "reposter_did"    : repost["did"],
	                    "reposter_handle" : repost["handle"]
	                })
	    edges = pd.DataFrame (rows)
	    g = edges.groupby (["orig_did", "orig_handle"])
	    nodes = pd.DataFrame ({"count" : g.size ()}).reset_index ()
	    nodes = nodes[["orig_did", "orig_handle", "count"]]
	    nodes["radius"] = np.sqrt (nodes["count"])
	    if node_colorizer:
	        nodes["color"] = nodes.apply (node_colorizer, axis=1)
	    df = edges.merge (nodes, on=["orig_did", "orig_handle"])
	    if node_colorizer:
	        if edge_colorizer:
	            df["edge_color"] = df.apply (edge_colorizer, axis=1)
	        else:
	            df["edge_color"] = df["color"]
	    if normalize_handles:
	        dids = {}
	        for i, r in df.iterrows ():
	            did = r["orig_did"]
	            if did not in dids:
	                dids[did] = r["orig_handle"]
	            did = r["reposter_did"]
	            if did not in dids:
	                dids[did] = r["reposter_handle"]
	        df["orig_handle"] = df["orig_did"].apply (lambda i: dids[i])
	        df["reposter_handle"] = df["reposter_did"].apply (
	                lambda i: dids[i])       
	    return df


	def by_repost_count (row):
	    green = min (row["count"] // 5, 255)
	    pink = hex (max (16, 255 - green))[2:]
	    return "#" + pink + hex (max (16, green))[2:] + pink


