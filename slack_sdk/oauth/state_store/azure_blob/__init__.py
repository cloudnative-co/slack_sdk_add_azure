import logging
import time
from logging import Logger
from uuid import uuid4

from azure.storage.blob._blob_service_client import BlobServiceClient
from slack_sdk.oauth.state_store.async_state_store import AsyncOAuthStateStore
from slack_sdk.oauth.state_store.state_store import OAuthStateStore


class AzureBlobOAuthStateStore(OAuthStateStore, AsyncOAuthStateStore):
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
        response = self.blob_service_client.put_object(
            Bucket=self.container_name,
            Body=str(time.time()),
            Key=state,
        )
        self.logger.debug(f"S3 put_object response: {response}")
        return state

    def consume(self, state: str) -> bool:
        try:
            fetch_response = self.blob_service_client.get_object(
                Bucket=self.container_name,
                Key=state,
            )
            self.logger.debug(f"S3 get_object response: {fetch_response}")
            body = fetch_response["Body"].read().decode("utf-8")
            created = float(body)
            expiration = created + self.expiration_seconds
            still_valid: bool = time.time() < expiration

            deletion_response = self.blob_service_client.delete_object(
                Bucket=self.container_name,
                Key=state,
            )
            self.logger.debug(f"S3 delete_object response: {deletion_response}")
            return still_valid
        except Exception as e:  # skipcq: PYL-W0703
            message = f"Failed to find any persistent data for state: {state} - {e}"
            self.logger.warning(message)
            return False
