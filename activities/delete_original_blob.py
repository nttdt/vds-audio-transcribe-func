# activities/delete_original_blob.py
from urllib.parse import urlparse

import azure.durable_functions as df
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient
from azure.core.exceptions import ResourceNotFoundError

bp_delete_original_blob = df.Blueprint()


def _parse_blob_url(blob_url: str):
    u = urlparse(blob_url)
    parts = u.path.lstrip("/").split("/", 1)
    account_url = f"{u.scheme}://{u.netloc}"
    return account_url, parts[0], parts[1]


@bp_delete_original_blob.activity_trigger(input_name="arg")
def DeleteOriginalBlob(arg: dict) -> str:
    blob_url = arg["blob_url"]
    account_url, container, blob_name = _parse_blob_url(blob_url)

    cred = DefaultAzureCredential()
    blob = BlobClient(account_url=account_url, container_name=container, blob_name=blob_name, credential=cred)

    try:
        blob.delete_blob(delete_snapshots="include")
        return "deleted"
    except ResourceNotFoundError:
        # idempotent: already deleted
        return "already_deleted"
