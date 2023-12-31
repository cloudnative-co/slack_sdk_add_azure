import json
import logging
from logging import Logger
from azure.storage.blob._blob_service_client import BlobServiceClient


class BlobStore(object):

    def __init__(
        self,
        *,
        client: BlobServiceClient,
        container_name: str,
        logger: Logger = logging.getLogger(__name__)
    ):
        self.container = client.get_container_client(
            container=container_name
        )
        if not self.container.exists():
            self.container.create_container()

    def upload(self, blob: str, data):
        response = self.container.upload_blob(name=blob, data=data, overwrite=True)
        return response

    def download(self, blob: str, is_json=True):
        response = self.container.download_blob(blob)
        body = response.readall().decode("utf-8")
        data = ""
        if is_json:
            data = json.loads(body)
        else:
            data = body
        return data

    def delete(self, blob: str):
        response = self.container.delete_blob(blob=blob, delete_snapshots="include")
        return response

    def list(self, prefix: str):
        response = self.container.list_blobs(name_starts_with=prefix)
        return response
