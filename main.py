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
# 請確保環境變數中已設定以下資訊
LINE_TOKEN = os.environ.get("LINE_TOKEN")
LINE_SECRET = os.environ.get("LINE_SECRET")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
DB_FILE = "bridge_queue.db"

# 使用 Session 複用 TCP 連線，提升大檔案傳輸效率
session = requests.Session()
# 使用 Event 物件來即時喚醒 Worker，不再死等 2 秒
task_event = threading.Event()

# --- 2. 資料庫核心機制 ---
def get_db_connection():
    """取得資料庫連線，設定 30 秒超時防止併發寫入時鎖定"""
    return sqlite3.connect(DB_FILE, timeout=30)

def init_db():
    """初始化 SQLite 資料庫，確保任務持久化"""
    print(f"正在檢查資料庫與資料表：{DB_FILE}")
    try:
        with get_db_connection() as conn:
            # 建立處理紀錄表 (防止重複)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_ids (
                    msg_id TEXT PRIMARY KEY, 
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 建立任務排隊表 (防止漏傳)
            # 新增 last_retry_at 欄位用來實施失敗冷卻機制
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
            # 自癒：將上次因故中斷的任務設回等待中
            conn.execute("UPDATE tasks SET status = 'pending' WHERE status = 'processing'")
            conn.commit()
        print("✅ 資料庫初始化完成")
    except Exception as e:
        print(f"❌ [資料庫初始化失敗] {e}")

def save_task_safely(msg_id, task_data):
    """將任務存入排隊清單，具備併發安全與重複檢查"""
    try:
        with get_db_connection() as conn:
            conn.execute("INSERT INTO processed_ids (msg_id) VALUES (?)", (msg_id,))
            conn.execute("INSERT INTO tasks (msg_id, payload) VALUES (?, ?)", (msg_id, json.dumps(task_data)))
            conn.commit()
            print(f"📥 任務已加入排隊: {msg_id}")
            # 任務加入後，立刻通知 Worker 起來工作 (喚醒機制)
            task_event.set()
            return True
    except sqlite3.IntegrityError:
        return False # 訊息 ID 重複
    except Exception as e:
        print(f"❌ [資料庫寫入錯誤] {e}")
        return False

# --- 3. 轉發邏輯 (下載與發送) ---
def verify_line_signature(body, signature):
    """驗證 LINE Webhook 簽章"""
    if not LINE_SECRET: return True
    if not signature: return False
    hash = hmac.new(LINE_SECRET.encode('utf-8'), body, hashlib.sha256).digest()
    expected_sig = base64.b64encode(hash).decode('utf-8')
    return hmac.compare_digest(expected_sig, signature)

def download_and_forward(task_data):
    """串流下載並轉傳大檔案，減少記憶體佔用"""
    msg_id = task_data["msg_id"]
    file_name = task_data["file_name"]
    line_url = f"https://api-data.line.me/v2/bot/message/{msg_id}/content"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}"}

    try:
        start_time = time.time()
        # stream=True 是處理數 MB PDF 的關鍵
        with session.get(line_url, headers=headers, timeout=30, stream=True) as r:
            if r.status_code != 200:
                print(f"❌ [LINE 下載失敗] 狀態碼: {r.status_code}, ID: {msg_id}")
                return False, 0
            
            download_done = time.time()
            
            # 決定 Telegram 方法
            method = "sendDocument"
            field = "document"
            orig_type = task_data.get("original_type")
            if orig_type == "image": method, field = "sendPhoto", "photo"
            elif orig_type == "video": method, field = "sendVideo", "video"
            elif orig_type == "audio": method, field = "sendAudio", "audio"

            files = {field: (file_name, r.raw, "application/octet-stream")}
            resp = session.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}",
                data={"chat_id": TELEGRAM_CHAT_ID},
                files=files,
                timeout=180 # 增加上傳逾時寬限
            )
            
            end_time = time.time()
            dl_dur = round(download_done - start_time, 1)
            ul_dur = round(end_time - download_done, 1)

            if resp.status_code == 200:
                print(f"✅ [成功] {file_name} (下:{dl_dur}s/上:{ul_dur}s)")
                return True, 0
            elif resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                print(f"⏳ [限流] Telegram 429，需等待 {wait} 秒")
                return False, wait
            else:
                print(f"❌ [TG 錯誤] {resp.status_code}: {resp.text[:200]}")
                return False, 0
    except Exception as e:
        print(f"❌ [傳輸異常] {e}")
        return False, 0

# --- 4. 背景 Worker (核心排隊引擎) ---
def worker():
    """單執行緒背景服務，確保排隊穩定且不卡死"""
    print("[Worker] 服務啟動成功，支援即時喚醒與失敗任務冷卻機制")
    while True:
        task = None
        try:
            now = int(time.time())
            with get_db_connection() as conn:
                cursor = conn.cursor()
                # 關鍵邏輯：優先處理重試次數少且「不在冷卻中」的任務
                # 失敗過的任務會排在後方，防止單一壞檔案卡死整個佇列
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
                    # 文字訊息處理
                    resp = session.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id": TELEGRAM_CHAT_ID, "text": task["data"]["text"]},
                        timeout=20
                    )
                    success = (resp.status_code == 200)
                    if not success and resp.status_code == 429:
                        wait_time = int(resp.headers.get("Retry-After", 30))
                else:
                    # 檔案訊息處理
                    success, wait_time = download_and_forward(task["data"])

                with get_db_connection() as conn:
                    if success:
                        conn.execute("DELETE FROM tasks WHERE id = ?", (task["db_id"],))
                    else:
                        # 失敗後更新重試次數與時間戳，讓它排到隊伍後方
                        conn.execute("""
                            UPDATE tasks SET 
                            status = 'pending', 
                            retry_count = retry_count + 1, 
                            last_retry_at = ? 
                            WHERE id = ?
                        """, (int(time.time()), task["db_id"]))
                    conn.commit()
                
                # 若觸發了限流則必須等待，否則維持 1 秒的基礎安全間隔
                time.sleep(max(wait_time, 1.0))
            else:
                # 沒任務時進入等待，直到被 Webhook 喚醒或 15 秒超時檢查一次
                task_event.wait(timeout=15)
                task_event.clear()

        except Exception as e:
            print(f"⚠️ [Worker 嚴重錯誤] {e}")
            time.sleep(5)

# --- 5. 網頁路徑 (Routes) ---

@app.route("/", methods=["GET"])
def index():
    """首頁：供 UptimeRobot 監控使用，防止 Render 休眠"""
    return "Bridge Service is active", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    """LINE Webhook 入口"""
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
                ext = {"image": "jpg", "video": "mp4", "audio": "m4a"}.get(msg_type, "pdf")
                task_data.update({
                    "file_name": msg.get("fileName") or f"{msg_id}.{ext}",
                    "original_type": msg_type
                })
            else:
                continue

            save_task_safely(msg_id, task_data)
            
    except Exception as e:
        print(f"💥 [Webhook 解析錯誤] {e}")

    return "OK", 200

@app.route("/status")
def status():
    """查看目前佇列進度"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
            counts = dict(cursor.fetchall())
        return jsonify({
            "pending_tasks": counts.get("pending", 0),
            "processing_now": counts.get("processing", 0),
            "info": "Sequential transfer with failed-task de-prioritization active."
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- 6. 啟動區 ---

# 強制在全域執行初始化
init_db()

# 檢查是否已啟動背景執行緒 (避免 Flask Reload 造成重複啟動)
if not any(t.name == "WorkerThread" for t in threading.enumerate()):
    worker_thread = threading.Thread(target=worker, name="WorkerThread", daemon=True)
    worker_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
