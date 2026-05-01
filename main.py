import os
import time
import json
import sqlite3
import threading
import hashlib
import hmac
import base64
import requests
import gc  # 導入垃圾回收模組
from flask import Flask, request, jsonify, abort

app = Flask(__name__)

# --- 1. 配置區 ---
LINE_TOKEN = os.environ.get("LINE_TOKEN")
LINE_SECRET = os.environ.get("LINE_SECRET")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
DB_FILE = "bridge_queue.db"

# 使用 Session 複用 TCP 連線
session = requests.Session()
task_event = threading.Event()

# --- 2. 資料庫核心機制 ---
def get_db_connection():
    return sqlite3.connect(DB_FILE, timeout=30)

def init_db():
    try:
        with get_db_connection() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS processed_ids (msg_id TEXT PRIMARY KEY, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    msg_id TEXT UNIQUE,
                    payload TEXT,
                    status TEXT DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    last_retry_at INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("UPDATE tasks SET status = 'pending' WHERE status = 'processing'")
            conn.commit()
    except Exception as e:
        print(f"❌ [DB Init Error] {e}")

def save_task_safely(msg_id, task_data):
    try:
        with get_db_connection() as conn:
            conn.execute("INSERT INTO processed_ids (msg_id) VALUES (?)", (msg_id,))
            conn.execute("INSERT INTO tasks (msg_id, payload) VALUES (?, ?)", (msg_id, json.dumps(task_data)))
            conn.commit()
            task_event.set()
            return True
    except sqlite3.IntegrityError:
        return False
    except Exception as e:
        print(f"❌ [DB Write Error] {e}")
        return False

# --- 3. 轉發邏輯 (記憶體優化版) ---
def verify_line_signature(body, signature):
    if not LINE_SECRET: return True
    if not signature: return False
    hash = hmac.new(LINE_SECRET.encode('utf-8'), body, hashlib.sha256).digest()
    expected_sig = base64.b64encode(hash).decode('utf-8')
    return hmac.compare_digest(expected_sig, signature)

def download_and_forward(task_data):
    msg_id = task_data["msg_id"]
    file_name = task_data["file_name"]
    line_url = f"https://api-data.line.me/v2/bot/message/{msg_id}/content"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}"}

    try:
        # 下載時嚴格使用 stream=True
        with session.get(line_url, headers=headers, timeout=30, stream=True) as r:
            if r.status_code != 200:
                return False, 0
            
            method = "sendDocument"
            field = "document"
            orig_type = task_data.get("original_type")
            if orig_type == "image": method, field = "sendPhoto", "photo"
            elif orig_type == "video": method, field = "sendVideo", "video"

            # 串流上傳至 Telegram
            files = {field: (file_name, r.raw, "application/octet-stream")}
            resp = session.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}",
                data={"chat_id": TELEGRAM_CHAT_ID},
                files=files,
                timeout=180
            )

            result = (resp.status_code == 200)
            wait_time = int(resp.headers.get("Retry-After", 0)) if resp.status_code == 429 else 0
            
            if result:
                print(f"✅ 已傳送: {file_name}")
            else:
                print(f"❌ TG 錯誤: {resp.status_code}")
                
            return result, wait_time
    except Exception as e:
        print(f"❌ 傳輸異常: {e}")
        return False, 0
    finally:
        # 關鍵：處理完大檔案後手動清理記憶體
        gc.collect()

# --- 4. 背景 Worker ---
def worker():
    while True:
        task = None
        try:
            now = int(time.time())
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, msg_id, payload FROM tasks 
                    WHERE status = 'pending' AND last_retry_at < ? - 10
                    ORDER BY retry_count ASC, id ASC LIMIT 1
                """, (now,))
                row = cursor.fetchone()

                if row:
                    task = {"db_id": row[0], "msg_id": row[1], "data": json.loads(row[2])}
                    conn.execute("UPDATE tasks SET status = 'processing' WHERE id = ?", (task["db_id"],))
                    conn.commit()

            if task:
                success, wait_time = False, 0
                if task["data"]["type"] == "text":
                    resp = session.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id": TELEGRAM_CHAT_ID, "text": task["data"]["text"]},
                        timeout=20
                    )
                    success = (resp.status_code == 200)
                    wait_time = int(resp.headers.get("Retry-After", 0)) if resp.status_code == 429 else 0
                else:
                    success, wait_time = download_and_forward(task["data"])

                with get_db_connection() as conn:
                    if success:
                        conn.execute("DELETE FROM tasks WHERE id = ?", (task["db_id"],))
                    else:
                        conn.execute("""
                            UPDATE tasks SET status = 'pending', retry_count = retry_count + 1, last_retry_at = ? 
                            WHERE id = ?
                        """, (int(time.time()), task["db_id"]))
                    conn.commit()
                
                # 任務結束後清理記憶體
                gc.collect()
                time.sleep(max(wait_time, 1.0))
            else:
                task_event.wait(timeout=15)
                task_event.clear()
        except Exception as e:
            print(f"⚠️ Worker 錯誤: {e}")
            time.sleep(5)

# --- 5. 網頁路徑 ---

@app.route("/", methods=["GET"])
def index():
    return "Bridge Service Active", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data()
    if not verify_line_signature(body, signature):
        abort(400)

    try:
        data = request.json
        events = data.get("events", [])
        for event in events:
            if event.get("type") != "message": continue
            msg = event.get("message", {})
            msg_id, msg_type = msg.get("id"), msg.get("type")

            task_data = {"msg_id": msg_id, "type": "text" if msg_type == "text" else "file"}
            if msg_type == "text":
                task_data["text"] = msg.get("text", "")
            elif msg_type in ["file", "image", "video", "audio"]:
                task_data.update({
                    "file_name": msg.get("fileName") or f"{msg_id}.pdf",
                    "original_type": msg_type
                })
            else: continue

            save_task_safely(msg_id, task_data)
    except: pass
    return "OK", 200

@app.route("/status")
def status():
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
            counts = dict(cursor.fetchall())
        return jsonify({"pending": counts.get("pending", 0), "processing": counts.get("processing", 0)})
    except:
        return jsonify({"error": "db_busy"}), 500

# --- 6. 啟動 ---
init_db()
if not any(t.name == "WorkerThread" for t in threading.enumerate()):
    threading.Thread(target=worker, name="WorkerThread", daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
