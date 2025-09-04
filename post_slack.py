import os, json, requests

def post_to_slack(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        raise RuntimeError("SLACK_WEBHOOK_URL not set")
    payload = {"text": text}
    r = requests.post(url, data=json.dumps(payload), headers={"Content-Type": "application/json"})
    r.raise_for_status()
