# activities/merge_transcripts.py
import azure.durable_functions as df

bp_merge_transcripts = df.Blueprint()


@bp_merge_transcripts.activity_trigger(input_name="arg")
def MergeTranscripts(arg: dict) -> str:
    texts = arg.get("texts", [])
    return "\n".join(texts)
