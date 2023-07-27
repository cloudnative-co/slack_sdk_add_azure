import logging
import time
from logging import Logger
from uuid import uuid4

from slack_sdk_azure.oauth.state_util.blob_store import BlobStore
from azure.storage.blob._blob_service_client import BlobServiceClient

from slack_sdk.oauth.state_store.async_state_store import AsyncOAuthStateStore
from slack_sdk.oauth.state_store.state_store import OAuthStateStore


class AzureBlobOAuthStateStore(OAuthStateStore, AsyncOAuthStateStore, BlobStore):
    def __init__(
        self,
        *,
        client: BlobServiceClient,
        container_name: str,
        expiration_seconds: int,
        logger: Logger = logging.getLogger(__name__),
    ):
        self.container_client = client.get_container_client(container_name)
        self.expiration_seconds = expiration_seconds
        self._logger = logger

    @property
    def logger(self) -> Logger:
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
        return self._logger

    async def async_issue(self, *args, **kwargs) -> str:
        return self.issue(*args, **kwargs)

    async def async_consume(self, state: str) -> bool:
        return self.consume(state)

    def issue(self, *args, **kwargs) -> str:
        state = str(uuid4())
        response = self.upload(blob=state, data=str(time.time()))
        return state

    def consume(self, state: str) -> bool:
        try:
            body = self.download(blob=state, is_json=False)
            created = float(body)
            expiration = created + self.expiration_seconds
            still_valid: bool = time.time() < expiration

            deletion_response = self.delete(state)
            return still_valid
        except Exception as e:  # skipcq: PYL-W0703
            message = f"Failed to find any persistent data for state: {state} - {e}"
            self.logger.warning(message)
            return False
