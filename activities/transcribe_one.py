# activities/transcribe_one.py
import os

import azure.durable_functions as df
import requests

bp_transcribe_one = df.Blueprint()


@bp_transcribe_one.activity_trigger(input_name="arg")
def TranscribeOne(arg: dict) -> str:
    path = arg["path"]

    endpoint = os.environ["AZURE_OPENAI_ENDPOINT"].rstrip("/")
    api_version = os.environ["AZURE_OPENAI_API_VERSION"]
    deployment = os.environ["AOAI_TRANSCRIBE_DEPLOYMENT"]
    api_key = os.environ["AZURE_OPENAI_KEY"]

    url = f"{endpoint}/openai/deployments/{deployment}/audio/transcriptions?api-version={api_version}"

    with open(path, "rb") as f:
        files = {"file": (os.path.basename(path), f)}
        headers = {"api-key": api_key}
        data = {"response_format": "json"}

        r = requests.post(url, headers=headers, files=files, data=data, timeout=600)
        r.raise_for_status()
        return r.json()["text"]
