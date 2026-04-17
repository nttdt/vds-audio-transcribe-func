# activities/download_blob.py
import os
import uuid
from urllib.parse import urlparse

import azure.durable_functions as df
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient

bp_download_blob = df.Blueprint()


def _parse_blob_url(blob_url: str):
    u = urlparse(blob_url)
    # /container/blobpath...
    parts = u.path.lstrip("/").split("/", 1)
    account_url = f"{u.scheme}://{u.netloc}"
    container = parts[0]
    blob_name = parts[1]
    return account_url, container, blob_name


@bp_download_blob.activity_trigger(input_name="arg")
def DownloadBlob(arg: dict) -> str:
    blob_url = arg["blob_url"]
    account_url, container, blob_name = _parse_blob_url(blob_url)

    cred = DefaultAzureCredential()
    blob = BlobClient(account_url=account_url, container_name=container, blob_name=blob_name, credential=cred)

    out_path = f"/tmp/{uuid.uuid4()}_{os.path.basename(blob_name)}"
    with open(out_path, "wb") as f:
        f.write(blob.download_blob().readall())

    return out_path
