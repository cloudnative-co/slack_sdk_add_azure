import logging
import time
from logging import Logger
from uuid import uuid4

from azure.storage.blob._blob_service_client import BlobServiceClient
from azure.storage.blob._container_client import ContainerClient
from slack_sdk.oauth.state_store.async_state_store import AsyncOAuthStateStore
from slack_sdk.oauth.state_store.state_store import OAuthStateStore


class AzureBlobOAuthStateStore(OAuthStateStore, AsyncOAuthStateStore):

    _client: BlobServiceClient = None
    _container: ContainerClient = None
    _container_name: str = ""

    def __init__(
        self,
        *,
        client: BlobServiceClient,
        container_name: str,
        expiration_seconds: int,
        logger: Logger = logging.getLogger(__name__),
    ):
        self.expiration_seconds = expiration_seconds
        self._logger = logger
        self._client = client
        self._container_name = container_name
        self.container_init()

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

    def container_init(self):
        self._container = self.blob_service_client.get_container_client(
            container=self.container_name
        )
        if not self.container.exists():
            self.container.create_container()

    def upload(self, blob: str, data):
        response = self.container.upload_blob(name=blob, data=data, overwrite=True)
        return response

    def download(self, blob: str, is_json=True):
        data = None
        try:
            response = self.container.download_blob(blob)
            body = response.readall().decode("utf-8")
            if is_json:
                data = json.loads(body)
            else:
                data = body
        except azure.core.exceptions.ResourceNotFoundError as e:
            self.logger.warning(str(e))
            data = None
        return data

    def delete(self, blob: str):
        response = self.container.delete_blob(blob=blob, delete_snapshots="include")
        return response

    def list(self, prefix: str):
        response = self.container.list_blobs(name_starts_with=prefix)
        return response

    @property
    def blob_service_client(self) -> BlobServiceClient:
        return self._client

    @property
    def container_name(self) -> str:
        return self._container_name

    @property
    def container(self) -> ContainerClient:
        if self._container is None:
            self.container_init()
        return self._container
