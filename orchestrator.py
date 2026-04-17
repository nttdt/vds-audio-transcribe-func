# orchestrator.py
import azure.durable_functions as df

bp_orchestrator = df.Blueprint()


@bp_orchestrator.orchestration_trigger(context_name="context")
def Orchestrator(context: df.DurableOrchestrationContext):
    """
    Workflow:
      1) Download blob to local temp
      2) Preprocess audio (compress to <=25MB; if still >25MB, split)
      3) Fan-out transcribe chunks
      4) Merge transcripts
      5) Summarize minutes (text)
      6) Save transcript + summary to Blob
      7) Delete original wav
      8) Cleanup local temp files
    """
    inp = context.get_input()
    blob_url = inp["blob_url"]
    etag = inp.get("etag")

    # Retry for transient failures (storage/network/AOAI)
    retry = df.RetryOptions(first_retry_interval_in_milliseconds=10_000, max_number_of_attempts=5)
    retry.backoff_coefficient = 2.0

    local_audio = yield context.call_activity_with_retry("DownloadBlob", retry, {"blob_url": blob_url})

    prep = yield context.call_activity_with_retry("PreprocessAudio", retry, {"local_path": local_audio})
    chunk_paths = prep["paths"]

    # fan-out
    tasks = [
        context.call_activity_with_retry("TranscribeOne", retry, {"path": p})
        for p in chunk_paths
    ]
    transcripts = yield context.task_all(tasks)

    full_text = yield context.call_activity("MergeTranscripts", {"texts": transcripts})

    summary_text = yield context.call_activity_with_retry("SummarizeMinutes", retry, {"text": full_text})

    yield context.call_activity_with_retry("WriteOutputs", retry, {
        "blob_url": blob_url,
        "transcript": full_text,
        "summary_text": summary_text
    })

    # Delete original only after successful outputs
    yield context.call_activity_with_retry("DeleteOriginalBlob", retry, {"blob_url": blob_url, "etag": etag})

    # Cleanup temp files (including chunk dirs)
    yield context.call_activity("CleanupLocal", {"paths": [local_audio] + chunk_paths})

    return "completed"
