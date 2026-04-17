# function_app.py
import hashlib
import logging
from typing import Optional

import azure.functions as func
import azure.durable_functions as df

from orchestrator import bp_orchestrator
from activities.download_blob import bp_download_blob
from activities.preprocess_audio import bp_preprocess_audio
from activities.transcribe_one import bp_transcribe_one
from activities.merge_transcripts import bp_merge_transcripts
from activities.summarize_minutes import bp_summarize_minutes
from activities.write_outputs import bp_write_outputs
from activities.delete_original_blob import bp_delete_original_blob
from activities.cleanup_local import bp_cleanup_local

# Durable Functions app (Python v2)
app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# Register blueprints (required — blueprints are NOT auto-indexed)
app.register_functions(bp_orchestrator)
app.register_functions(bp_download_blob)
app.register_functions(bp_preprocess_audio)
app.register_functions(bp_transcribe_one)
app.register_functions(bp_merge_transcripts)
app.register_functions(bp_summarize_minutes)
app.register_functions(bp_write_outputs)
app.register_functions(bp_delete_original_blob)
app.register_functions(bp_cleanup_local)


def _instance_id(blob_url: str, etag: Optional[str]) -> str:
    """
    Deterministic instanceId to prevent duplicate processing.
    Event Grid can deliver duplicates; this makes orchestration start idempotent.
    """
    key = f"{blob_url}|{etag or ''}".encode("utf-8")
    return hashlib.sha256(key).hexdigest()


@app.function_name(name="BlobCreated_Starter")
@app.event_grid_trigger(arg_name="event")
@app.durable_client_input(client_name="client")
async def blobcreated_starter(
    event: func.EventGridEvent,
    client: df.DurableOrchestrationClient
) -> None:
    """
    Receives Microsoft.Storage.BlobCreated events and starts Durable Orchestrator.
    """
    try:
        event_type = (event.event_type or "").lower()
        if event_type != "microsoft.storage.blobcreated":
            logging.info("Skip eventType=%s", event.event_type)
            return

        data = event.get_json()
        blob_url = data["url"]
        etag = data.get("eTag")

        iid = _instance_id(blob_url, etag)

        status = await client.get_status(iid)
        if status and status.runtime_status not in ("Completed", "Failed", "Terminated"):
            logging.info("Orchestration already running: %s (status=%s)", iid, status.runtime_status)
            return

        await client.start_new(
            orchestration_function_name="Orchestrator",
            instance_id=iid,
            client_input={"blob_url": blob_url, "etag": etag},
        )
        logging.info("Started orchestration: %s", iid)

    except Exception as ex:
        logging.exception("Failed to start orchestration: %s", ex)
        raise
