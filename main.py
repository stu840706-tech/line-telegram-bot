import os
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

LINE_TOKEN = os.environ.get("LINE_TOKEN")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    events = data.get("events", [])
    
    for event in events:
        if event.get("type") != "message":
            continue
        
        message = event.get("message", {})
        msg_type = message.get("type")
        msg_id = message.get("id")
        
        if msg_type == "text":
            text = message.get("text", "")
            send_telegram_text(text)
        
        elif msg_type in ["file", "image", "video", "audio"]:
            file_name = message.get("fileName", f"{msg_id}.bin")
            download_and_send(msg_id, file_name)
    
    return jsonify({"status": "ok"}), 200

def download_and_send(msg_id, file_name):
    url = f"https://api-data.line.me/v2/bot/message/{msg_id}/content"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}"}
    r = requests.get(url, headers=headers)
    
    if r.status_code == 200:
        files = {"document": (file_name, r.content)}
        data = {"chat_id": TELEGRAM_CHAT_ID}
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
            data=data, files=files
        )

def send_telegram_text(text):
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        data=data
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)