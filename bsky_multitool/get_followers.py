from typing import Optional
import atproto
from .utils import get_client

class getFollowers:
    def __init__(
        self,
        handle:             Optional[str]            = None,
        app_password:       Optional[str]            = None,
        client:             Optional[atproto.Client] = None
    ):
        self.client = get_client(handle, app_password, client)
    
    def get_followers(self, did_or_handle: str) -> list[dict]:
        did_or_handle = did_or_handle  # or use their DID
        cursor = None
        followers = []

        while True:
            resp = self.client.app.bsky.graph.get_followers({
                'actor': did_or_handle,
                'cursor': cursor,
                'limit': 100  # max per request
            })
            followers.extend(
                [follower.model_dump() for follower in resp.followers]
            )
            if not resp.cursor:
                break
            cursor = resp.cursor

        return followers