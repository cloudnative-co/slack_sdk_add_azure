import azure
import json
import logging
from logging import Logger
from typing import Optional
from azure.storage.blob._blob_service_client import BlobServiceClient
from azure.storage.blob._container_client import ContainerClient
from slack_sdk.errors import SlackClientConfigurationError
from slack_sdk.oauth.installation_store.async_installation_store import (
    AsyncInstallationStore,
)
from slack_sdk.oauth.installation_store.installation_store import InstallationStore
from slack_sdk.oauth.installation_store.models.bot import Bot
from slack_sdk.oauth.installation_store.models.installation import Installation


class AzureBlobInstallationStore(InstallationStore, AsyncInstallationStore):

    _client: BlobServiceClient = None
    _container: ContainerClient = None
    _container_name: str = ""

    def __init__(
        self,
        *,
        client: BlobServiceClient,
        container_name: str,
        client_id: str,
        historical_data_enabled: bool = True,
        logger: Logger = logging.getLogger(__name__),
    ):
        self.historical_data_enabled = historical_data_enabled
        self.client_id = client_id
        self._logger = logger
        self._client = client
        self._container_name = container_name
        self.container_init()

    @property
    def logger(self) -> Logger:
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
        return self._logger

    async def async_save(self, installation: Installation):
        return self.save(installation)

    async def async_save_bot(self, bot: Bot):
        return self.save_bot(bot)

    def save(self, installation: Installation):
        none = "none"
        e_id = installation.enterprise_id or none
        t_id = installation.team_id or none
        workspace_path = f"{self.client_id}/{e_id}-{t_id}"

        self.save_bot(installation.to_bot())

        if self.historical_data_enabled:
            history_version: str = str(installation.installed_at)
            # per workspace
            entity: str = json.dumps(installation.__dict__)
            self.upload(blob=f"{workspace_path}/installer-latest", data=entity)
            self.upload(blob=f"{workspace_path}/installer-{history_version}", data=entity)
            # per workspace per user
            u_id = installation.user_id or none
            entity: str = json.dumps(installation.__dict__)
            self.upload(blob=f"{workspace_path}/installer-{u_id}-latest", data=entity)
            self.upload(blob=f"{workspace_path}/installer-{u_id}-{history_version}", data=entity)
        else:
            # per workspace
            entity: str = json.dumps(installation.__dict__)
            self.upload(blob=f"{workspace_path}/installer-latest", data=entity)

            # per workspace per user
            u_id = installation.user_id or none
            entity: str = json.dumps(installation.__dict__)
            self.upload(blob=f"{workspace_path}/installer-{u_id}-latest", data=entity)


    def save_bot(self, bot: Bot):
        none = "none"
        e_id = bot.enterprise_id or none
        t_id = bot.team_id or none
        workspace_path = f"{self.client_id}/{e_id}-{t_id}"

        if self.historical_data_enabled:
            history_version: str = str(bot.installed_at)
            entity: str = json.dumps(bot.__dict__)
            self.upload(blob=f"{workspace_path}/bot-latest", data=entity)
            self.upload(blob=f"{workspace_path}/bot-{history_version}", data=entity)
        else:
            entity: str = json.dumps(bot.__dict__)
            self.upload(blob=f"{workspace_path}/bot-latest", data=entity)

    async def async_find_bot(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        is_enterprise_install: Optional[bool] = False,
    ) -> Optional[Bot]:
        return self.find_bot(
            enterprise_id=enterprise_id,
            team_id=team_id,
            is_enterprise_install=is_enterprise_install,
        )

    def find_bot(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        is_enterprise_install: Optional[bool] = False,
    ) -> Optional[Bot]:
        none = "none"
        e_id = enterprise_id or none
        t_id = team_id or none
        if is_enterprise_install:
            t_id = none
        workspace_path = f"{self.client_id}/{e_id}-{t_id}"
        try:
            data = self.download(f"{workspace_path}/bot-latest")
            return Bot(**data)
        except Exception as e:  # skipcq: PYL-W0703
            message = f"Failed to find bot installation data for enterprise: {e_id}, team: {t_id}: {e}"
            self.logger.warning(message)
            return None

    async def async_find_installation(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        user_id: Optional[str] = None,
        is_enterprise_install: Optional[bool] = False,
    ) -> Optional[Installation]:
        return self.find_installation(
            enterprise_id=enterprise_id,
            team_id=team_id,
            user_id=user_id,
            is_enterprise_install=is_enterprise_install,
        )

    def find_installation(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        user_id: Optional[str] = None,
        is_enterprise_install: Optional[bool] = False,
    ) -> Optional[Installation]:

        none = "none"
        e_id = enterprise_id or none
        t_id = team_id or none
        if is_enterprise_install:
            t_id = none
        workspace_path = f"{self.client_id}/{e_id}-{t_id}"
        try:
            key = f"{workspace_path}/installer-{user_id}-latest" if user_id else f"{workspace_path}/installer-latest"
            data = self.download(key)
            if data is None:
                data = {}
            installation = Installation(**data)
            if installation is not None and user_id is not None:
                # Retrieve the latest bot token, just in case
                # See also: https://github.com/slackapi/bolt-python/issues/664
                latest_bot_installation = self.find_installation(
                    enterprise_id=enterprise_id,
                    team_id=team_id,
                    is_enterprise_install=is_enterprise_install,
                )
                if latest_bot_installation is not None and installation.bot_token != latest_bot_installation.bot_token:
                    # NOTE: this logic is based on the assumption that every single installation has bot scopes
                    # If you need to installation patterns without bot scopes in the same S3 bucket,
                    # please fork this code and implement your own logic.
                    installation.bot_id = latest_bot_installation.bot_id
                    installation.bot_user_id = latest_bot_installation.bot_user_id
                    installation.bot_token = latest_bot_installation.bot_token
                    installation.bot_scopes = latest_bot_installation.bot_scopes
                    installation.bot_refresh_token = latest_bot_installation.bot_refresh_token
                    installation.bot_token_expires_at = latest_bot_installation.bot_token_expires_at

            return installation

        except Exception as e:  # skipcq: PYL-W0703
            self.logger.warning(f"Exception Type: {type(e)}")
            message = f"Failed to find an installation data for enterprise: {e_id}, team: {t_id}: {e}"
            self.logger.warning(message)
            raise e
            return None

    async def async_delete_bot(self, *, enterprise_id: Optional[str], team_id: Optional[str]) -> None:
        return self.delete_bot(
            enterprise_id=enterprise_id,
            team_id=team_id,
        )

    def delete_bot(self, *, enterprise_id: Optional[str], team_id: Optional[str]) -> None:
        none = "none"
        e_id = enterprise_id or none
        t_id = team_id or none
        workspace_path = f"{self.client_id}/{e_id}-{t_id}"

        objects = self.list(prefix=f"{workspace_path}/bot-")
        for content in objects:
            key = content.get("name")
            if key is not None:
                self.logger.info(f"Going to delete bot installation ({key})")
                try:
                    self.delete(blob=key)
                except Exception as e:  # skipcq: PYL-W0703
                    message = f"Failed to find bot installation data for enterprise: {e_id}, team: {t_id}: {e}"
                    raise SlackClientConfigurationError(message)

    async def async_delete_installation(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        user_id: Optional[str] = None,
    ) -> None:
        return self.delete_installation(
            enterprise_id=enterprise_id,
            team_id=team_id,
            user_id=user_id,
        )

    def delete_installation(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        user_id: Optional[str] = None,
    ) -> None:
        none = "none"
        e_id = enterprise_id or none
        t_id = team_id or none
        workspace_path = f"{self.client_id}/{e_id}-{t_id}"
        objects = self.list(
            prefix=f"{workspace_path}/installer-{user_id or ''}",
        )
        deleted_keys = []
        for content in objects:
            key = content.get("name")
            if key is not None:
                self.logger.info(f"Going to delete installation ({key})")
                try:
                    self.delete(blob=key)
                    deleted_keys.append(key)
                except Exception as e:  # skipcq: PYL-W0703
                    message = f"Failed to find bot installation data for enterprise: {e_id}, team: {t_id}: {e}"
                    raise SlackClientConfigurationError(message)

                try:
                    no_user_id_key = key.replace(f"-{user_id}", "")
                    if not no_user_id_key.endswith("installer-latest"):
                        self.delete(blob=no_user_id_key)
                        deleted_keys.append(no_user_id_key)
                except Exception as e:  # skipcq: PYL-W0703
                    message = f"Failed to find bot installation data for enterprise: {e_id}, team: {t_id}: {e}"
                    raise SlackClientConfigurationError(message)

        # Check the remaining installation data
        objects = self.list(
            prefix=f"{workspace_path}/installer-",
#            MaxKeys=10,  # the small number would be enough for this purpose
        )
        keys = [c["name"] for c in objects if c["name"] not in deleted_keys]
        # If only installer-latest remains, we should delete the one as well
        if len(keys) == 1 and keys[0].endswith("installer-latest"):
            content = objects[0]
            try:
                self.delete(content["name"])
            except Exception as e:  # skipcq: PYL-W0703
                message = f"Failed to find bot installation data for enterprise: {e_id}, team: {t_id}: {e}"
                raise SlackClientConfigurationError(message)

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
