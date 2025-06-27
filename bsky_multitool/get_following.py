from typing import Optional
import atproto
from .utils import get_client

class getFollowing:
    def __init__(
        self,
        handle:             Optional[str]            = None,
        app_password:       Optional[str]            = None,
        client:             Optional[atproto.Client] = None
    ):
        self.client = get_client(handle, app_password, client)
    
    def get_following(self, did_or_handle: str) -> list[dict]:
        did_or_handle = did_or_handle  # or use their DID
        cursor = None
        following = []

        while True:
            resp = self.client.app.bsky.graph.get_follows({
                'actor': did_or_handle,
                'cursor': cursor,
                'limit': 100  # max per request
            })
            following.extend(
                [following.model_dump() for following in resp.follows]
            )
            if not resp.cursor:
                break
            cursor = resp.cursor

        return following