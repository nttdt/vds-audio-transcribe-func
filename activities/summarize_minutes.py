# activities/summarize_minutes.py
import os

import azure.durable_functions as df
import requests

bp_summarize_minutes = df.Blueprint()

SYSTEM_PROMPT = """あなたは議事録作成の専門家です。
以下の全文文字起こしから、必ず次の形式で日本語の議事録要約を作成してください。

【件名】
（推定でOK。短く）

【概要】
- 3〜5行で要点

【決定事項】
- 決定事項を箇条書き（なければ「- なし」）

【ToDo】
- 「タスク / 担当 / 期限」を可能な範囲で書く。担当や期限が不明なら「未確定」と明記。
  例: - タスク: ◯◯ / 担当: 未確定 / 期限: 未確定
  なければ「- なし」

【論点・未決事項】
- 未解決の論点を箇条書き（なければ「- なし」）

【リスク・注意点】
- リスクや注意点を箇条書き（なければ「- なし」）

【次回】
- 次回予定や次アクション（不明なら「- 未定」）

制約:
- 必ず上記の見出しを含め、見出し名は変更しない。
- 事実に基づき、推測は「推定」と明記する。
"""


@bp_summarize_minutes.activity_trigger(input_name="arg")
def SummarizeMinutes(arg: dict) -> str:
    text = arg["text"]

    endpoint = os.environ["AZURE_OPENAI_ENDPOINT"].rstrip("/")
    api_version = os.environ["AZURE_OPENAI_API_VERSION"]
    deployment = os.environ["AOAI_SUMMARY_DEPLOYMENT"]
    api_key = os.environ["AZURE_OPENAI_KEY"]

    url = f"{endpoint}/openai/deployments/{deployment}/chat/completions?api-version={api_version}"

    payload = {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
        "temperature": 0.2,
        "max_tokens": 1200
    }

    r = requests.post(url, headers={"api-key": api_key}, json=payload, timeout=600)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]
