import os
import time
import json
import threading
import requests
from flask import Flask, request, jsonify
from queue import Queue

app = Flask(__name__)

LINE_TOKEN = os.environ.get("LINE_TOKEN")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# 持久化 processed_ids 到磁碟，防止重啟後重複
PROCESSED_IDS_FILE = "processed_ids.json"

def load_processed_ids():
    try:
        with open(PROCESSED_IDS_FILE, "r") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def save_processed_ids(ids):
    # 只保留最近 5000 筆，防止無限增長
    recent = list(ids)[-5000:]
    with open(PROCESSED_IDS_FILE, "w") as f:
        json.dump(recent, f)

processed_ids = load_processed_ids()
processed_lock = threading.Lock()

# 工作佇列：webhook 把任務放進去，worker thread 非同步處理
task_queue = Queue()

def worker():
    """背景 worker，從佇列取任務執行，每次發送後等 0.5 秒避免 Telegram rate limit"""
    while True:
        task = task_queue.get()
        try:
            msg_type = task["type"]
            if msg_type == "text":
                retry(send_telegram_text, task["text"])
            elif msg_type == "file":
                retry(download_and_send, task["msg_id"], task["file_name"])
        except Exception as e:
            print(f"[Worker Error] {e}")
        finally:
            task_queue.task_done()
        time.sleep(0.5)  # 每筆間隔 0.5 秒，避免 Telegram 30 msg/sec 限制

# 啟動背景 worker（daemon=True 讓主程式退出時自動結束）
threading.Thread(target=worker, daemon=True).start()

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "queue_size": task_queue.qsize()}), 200

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        return jsonify({"status": "ok"}), 200

    data = request.json
    events = data.get("events", [])
    queued = 0

    for event in events:
        if event.get("type") != "message":
            continue

        message = event.get("message", {})
        msg_type = message.get("type")
        msg_id = message.get("id")

        # 防重複
        with processed_lock:
            if msg_id in processed_ids:
                continue
            processed_ids.add(msg_id)
            save_processed_ids(processed_ids)

        if msg_type == "text":
            task_queue.put({"type": "text", "text": message.get("text", "")})
            queued += 1
        elif msg_type in ["file", "image", "video", "audio"]:
            file_name = message.get("fileName", f"{msg_id}.bin")
            task_queue.put({"type": "file", "msg_id": msg_id, "file_name": file_name})
            queued += 1

    # 立即回傳 200，不等處理完成
    return jsonify({"status": "ok", "queued": queued}), 200

def retry(func, *args, max_retries=5, base_delay=2):
    for attempt in range(max_retries):
        try:
            result = func(*args)
            if result:
                return True
            # result=False 代表非 429 的失敗，直接重試
        except Exception as e:
            print(f"[Retry {attempt+1}/{max_retries}] {func.__name__} 失敗：{e}")

        if attempt < max_retries - 1:
            # Exponential backoff：2, 4, 8, 16 秒
            wait = base_delay * (2 ** attempt)
            print(f"等待 {wait} 秒後重試...")
            time.sleep(wait)

    print(f"[Retry Failed] {func.__name__} 已重試 {max_retries} 次，放棄")
    return False

def download_and_send(msg_id, file_name):
    url = f"https://api-data.line.me/v2/bot/message/{msg_id}/content"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}"}

    # stream=True：邊下載邊傳，不把整個 PDF 載入記憶體
    with requests.get(url, headers=headers, timeout=30, stream=True) as r:
        if r.status_code != 200:
            print(f"[Download Failed] msg_id={msg_id}, status={r.status_code}")
            return False

        files = {"document": (file_name, r.raw, "application/octet-stream")}
        data = {"chat_id": TELEGRAM_CHAT_ID}
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
            data=data,
            files=files,
            timeout=60
        )

    if resp.status_code == 429:
        # 讀 Retry-After header，等指定秒數
        retry_after = int(resp.headers.get("Retry-After", 10))
        print(f"[Telegram 429] 等待 {retry_after} 秒")
        time.sleep(retry_after)
        return False  # 讓 retry() 重試

    if resp.status_code != 200:
        print(f"[Telegram Failed] {file_name}, status={resp.status_code}, body={resp.text[:200]}")
        return False

    print(f"[OK] 已發送：{file_name}")
    return True

def send_telegram_text(text):
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    resp = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        data=data,
        timeout=10
    )

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 5))
        time.sleep(retry_after)
        return False

    return resp.status_code == 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
