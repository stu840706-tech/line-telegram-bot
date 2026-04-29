import os
import time
import json
import sqlite3
import threading
import hashlib
import hmac
import base64
import requests
from flask import Flask, request, jsonify, abort

app = Flask(__name__)

# --- 1. 配置區 ---
LINE_TOKEN = os.environ.get("LINE_TOKEN")
LINE_SECRET = os.environ.get("LINE_SECRET")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
DB_FILE = "bridge_queue.db"

# --- 2. 資料庫核心機制 ---
def init_db():
    """初始化 SQLite 資料庫，確保任務持久化"""
    print(f"正在檢查資料庫與資料表：{DB_FILE}")
    with sqlite3.connect(DB_FILE) as conn:
        # 建立處理紀錄表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_ids (
                msg_id TEXT PRIMARY KEY, 
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # 建立任務排隊表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id TEXT UNIQUE,
                payload TEXT,
                status TEXT DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # 將上次因故中斷的任務設回等待中
        conn.execute("UPDATE tasks SET status = 'pending' WHERE status = 'processing'")
        conn.commit()
    print("✅ 資料庫初始化完成")

def save_task_safely(msg_id, task_data):
    """將任務存入排隊清單"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("INSERT INTO processed_ids (msg_id) VALUES (?)", (msg_id,))
            conn.execute("INSERT INTO tasks (msg_id, payload) VALUES (?, ?)", (msg_id, json.dumps(task_data)))
            conn.commit()
            return True
    except sqlite3.IntegrityError:
        print(f"💡 訊息 ID {msg_id} 已存在，跳過重複處理")
        return False
    except Exception as e:
        print(f"❌ [資料庫錯誤] {e}")
        return False

# --- 3. 轉發邏輯 ---
def verify_line_signature(body, signature):
    """驗證 LINE Webhook 簽章"""
    if not LINE_SECRET:
        return True
    if not signature:
        return False
    hash = hmac.new(LINE_SECRET.encode('utf-8'), body, hashlib.sha256).digest()
    expected_sig = base64.b64encode(hash).decode('utf-8')
    return hmac.compare_digest(expected_sig, signature)

def download_and_forward(task_data):
    """下載 LINE 內容並串流傳送給 Telegram"""
    msg_id = task_data["msg_id"]
    file_name = task_data["file_name"]
    line_url = f"https://api-data.line.me/v2/bot/message/{msg_id}/content"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}"}

    try:
        with requests.get(line_url, headers=headers, timeout=60, stream=True) as r:
            if r.status_code != 200:
                print(f"❌ [LINE 下載失敗] 狀態碼: {r.status_code}")
                return False

            method = "sendDocument"
            field = "document"
            orig_type = task_data.get("original_type")
            if orig_type == "image": method, field = "sendPhoto", "photo"
            elif orig_type == "video": method, field = "sendVideo", "video"
            elif orig_type == "audio": method, field = "sendAudio", "audio"

            files = {field: (file_name, r.raw, "application/octet-stream")}
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}",
                data={"chat_id": TELEGRAM_CHAT_ID},
                files=files,
                timeout=300
            )

            if resp.status_code == 200:
                print(f"✅ [轉傳成功] {file_name}")
                return True
            return False
    except Exception as e:
        print(f"❌ [傳輸異常] {e}")
        return False

# --- 4. 背景 Worker ---
def worker():
    """排隊處理 Worker"""
    print("[Worker] 背景服務啟動成功")
    while True:
        task = None
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, msg_id, payload FROM tasks WHERE status = 'pending' ORDER BY id ASC LIMIT 1")
                row = cursor.fetchone()
                if row:
                    task = {"db_id": row[0], "msg_id": row[1], "data": json.loads(row[2])}
                    conn.execute("UPDATE tasks SET status = 'processing' WHERE id = ?", (task["db_id"],))
                    conn.commit()

            if task:
                success = False
                if task["data"]["type"] == "text":
                    resp = requests.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id": TELEGRAM_CHAT_ID, "text": task["data"]["text"]},
                        timeout=20
                    )
                    success = (resp.status_code == 200)
                else:
                    success = download_and_forward(task["data"])

                with sqlite3.connect(DB_FILE) as conn:
                    if success:
                        conn.execute("DELETE FROM tasks WHERE id = ?", (task["db_id"],))
                    else:
                        conn.execute("UPDATE tasks SET status = 'pending', retry_count = retry_count + 1 WHERE id = ?", (task["db_id"],))
                    conn.commit()
                time.sleep(1.2)
            else:
                time.sleep(2)
        except Exception as e:
            print(f"⚠️ [Worker 嚴重錯誤] {e}")
            time.sleep(5)

# --- 5. Webhook 入口 ---
@app.route("/webhook", methods=["POST"])
def webhook():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data()
    
    if not verify_line_signature(body, signature):
        abort(400)

    data = request.json
    events = data.get("events", [])
    for event in events:
        if event.get("type") != "message":
            continue
        
        msg = event.get("message", {})
        msg_id, msg_type = msg.get("id"), msg.get("type")

        task_data = {"msg_id": msg_id, "type": "text" if msg_type == "text" else "file"}
        if msg_type == "text":
            task_data["text"] = msg.get("text", "")
        elif msg_type in ["file", "image", "video", "audio"]:
            ext = {"image": "jpg", "video": "mp4", "audio": "m4a"}.get(msg_type, "pdf")
            task_data.update({
                "file_name": msg.get("fileName") or f"{msg_id}.{ext}",
                "original_type": msg_type
            })
        else:
            continue

        save_task_safely(msg_id, task_data)

    return "OK", 200

@app.route("/status")
def status():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
        counts = dict(cursor.fetchall())
    return jsonify({"pending": counts.get("pending", 0), "processing": counts.get("processing", 0)})

# --- 6. 強制初始化 (放在最外層確保一定執行) ---
init_db()
if not any(t.name == "WorkerThread" for t in threading.enumerate()):
    worker_thread = threading.Thread(target=worker, name="WorkerThread", daemon=True)
    worker_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
