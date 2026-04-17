# activities/write_outputs.py
import os
from urllib.parse import urlparse

import azure.durable_functions as df
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

bp_write_outputs = df.Blueprint()


def _base_name(blob_url: str) -> str:
    u = urlparse(blob_url)
    blob_path = u.path.lstrip("/").split("/", 1)[1]
    return os.path.splitext(os.path.basename(blob_path))[0]


@bp_write_outputs.activity_trigger(input_name="arg")
def WriteOutputs(arg: dict) -> str:
    blob_url = arg["blob_url"]
    transcript = arg["transcript"]
    summary_text = arg["summary_text"]

    base = _base_name(blob_url)

    account_url = os.environ["STORAGE_ACCOUNT_URL"]
    text_container = os.environ["TEXT_CONTAINER"]
    summary_container = os.environ["SUMMARY_CONTAINER"]

    cred = DefaultAzureCredential()
    svc = BlobServiceClient(account_url=account_url, credential=cred)

    svc.get_blob_client(text_container, f"{base}.txt").upload_blob(
        transcript.encode("utf-8"), overwrite=True
    )
    svc.get_blob_client(summary_container, f"{base}.summary.txt").upload_blob(
        summary_text.encode("utf-8"), overwrite=True
    )
    return "ok"
