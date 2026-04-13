import os
import time
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

LINE_TOKEN = os.environ.get("LINE_TOKEN")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# 記錄已處理過的 message ID，防止重複傳送
processed_ids = set()

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        return jsonify({"status": "ok"}), 200
    
    data = request.json
    events = data.get("events", [])
    
    for event in events:
        if event.get("type") != "message":
            continue
        
        message = event.get("message", {})
        msg_type = message.get("type")
        msg_id = message.get("id")
        
        # 防重複：已處理過的直接跳過
        if msg_id in processed_ids:
            continue
        processed_ids.add(msg_id)
        
        if msg_type == "text":
            text = message.get("text", "")
            retry(send_telegram_text, text)
        
        elif msg_type in ["file", "image", "video", "audio"]:
            file_name = message.get("fileName", f"{msg_id}.bin")
            retry(download_and_send, msg_id, file_name)
    
    return jsonify({"status": "ok"}), 200

def retry(func, *args, max_retries=3, delay=2):
    for attempt in range(max_retries):
        try:
            result = func(*args)
            if result:
                return True
        except Exception as e:
            print(f"第 {attempt+1} 次嘗試失敗：{e}")
        if attempt < max_retries - 1:
            time.sleep(delay)
    print(f"已重試 {max_retries} 次，仍然失敗")
    return False

def download_and_send(msg_id, file_name):
    url = f"https://api-data.line.me/v2/bot/message/{msg_id}/content"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}"}
    r = requests.get(url, headers=headers, timeout=10)
    
    if r.status_code == 200:
        files = {"document": (file_name, r.content)}
        data = {"chat_id": TELEGRAM_CHAT_ID}
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
            data=data, files=files, timeout=30
        )
        return resp.status_code == 200
    return False

def send_telegram_text(text):
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    resp = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        data=data, timeout=10
    )
    return resp.status_code == 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
