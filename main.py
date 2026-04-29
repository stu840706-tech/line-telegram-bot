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

# --- 1. 配置區 (請確保在環境變數中設定這些數值) ---
LINE_TOKEN = os.environ.get("LINE_TOKEN")
LINE_SECRET = os.environ.get("LINE_SECRET")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
DB_FILE = "bridge_queue.db"  # 資料庫檔案路徑

# --- 2. 資料庫核心機制 (持久化與防重) ---
def init_db():
    """初始化 SQLite 資料庫，建立防重複表與任務表"""
    with sqlite3.connect(DB_FILE) as conn:
        # 紀錄已處理過的 LINE message_id，防止重複
        conn.execute("CREATE TABLE IF NOT EXISTS processed_ids (msg_id TEXT PRIMARY KEY, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        # 持久化任務佇列，確保重啟後任務不遺失
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
        # 啟動自癒：將上次因當機中斷的任務狀態設回 'pending'，重新開始排隊
        conn.execute("UPDATE tasks SET status = 'pending' WHERE status = 'processing'")
        conn.commit()

def save_task_safely(msg_id, task_data):
    """利用資料庫事務確保『檢查重複』與『排隊』的原子性"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            # 嘗試寫入 ID 表，若 ID 已存在會觸發 IntegrityError
            conn.execute("INSERT INTO processed_ids (msg_id) VALUES (?)", (msg_id,))
            # 寫入待處理任務排隊
            conn.execute("INSERT INTO tasks (msg_id, payload) VALUES (?, ?)", (msg_id, json.dumps(task_data)))
            conn.commit()
            return True
    except sqlite3.IntegrityError:
        # 代表 msg_id 已存在，這是一則重複的訊息，直接忽略
        return False
    except Exception as e:
        print(f"[資料庫錯誤] {e}")
        return False

# --- 3. 核心轉發邏輯 ---
def verify_line_signature(body, signature):
    """驗證 LINE Webhook 簽章，防止非法請求"""
    if not LINE_SECRET: return True
    hash = hmac.new(LINE_SECRET.encode('utf-8'), body, hashlib.sha256).digest()
    expected_sig = base64.b64encode(hash).decode('utf-8')
    return hmac.compare_digest(expected_sig, signature)

def download_and_forward(task_data):
    """串流下載並轉傳大檔案，減少記憶體佔用"""
    msg_id = task_data["msg_id"]
    line_url = f"https://api-data.line.me/v2/bot/message/{msg_id}/content"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}"}

    try:
        # stream=True 是處理數 MB PDF 的關鍵，避免檔案載入實體記憶體
        with requests.get(line_url, headers=headers, timeout=60, stream=True) as r:
            if r.status_code != 200:
                print(f"[下載失敗] LINE ID: {msg_id}, Code: {r.status_code}")
                return False

            # 判斷 Telegram 發送方法
            method = "sendDocument"
            field = "document"
            orig_type = task_data.get("original_type")
            if orig_type == "image": method, field = "sendPhoto", "photo"
            elif orig_type == "video": method, field = "sendVideo", "video"
            elif orig_type == "audio": method, field = "sendAudio", "audio"

            files = {field: (task_data["file_name"], r.raw, "application/octet-stream")}
            
            resp = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}",
                data={"chat_id": TELEGRAM_CHAT_ID},
                files=files,
                timeout=300  # 給予大檔案充足的上傳超時時間
            )

            if resp.status_code == 200:
                print(f"[成功] 已轉傳檔案: {task_data['file_name']}")
                return True
            elif resp.status_code == 429:
                # 碰到 Telegram 頻率限制，主動獲取建議等待時間
                wait = int(resp.headers.get("Retry-After", 30))
                print(f"[Telegram 429] 觸發限制，等待 {wait} 秒")
                time.sleep(wait)
                return False
            else:
                print(f"[Telegram 失敗] {resp.status_code}: {resp.text}")
                return False
    except Exception as e:
        print(f"[傳輸異常] {e}")
        return False

# --- 4. 背景 Worker (確保順序與排隊) ---
def worker():
    """單線程 Worker：嚴格一個接一個處理，確保不會對 Telegram 造成壓力"""
    print("[Worker] 背景服務啟動，開始監控任務...")
    while True:
        task = None
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                # 撈出最舊的一筆 pending 任務 (FIFO 先進先出)
                cursor.execute("SELECT id, msg_id, payload FROM tasks WHERE status = 'pending' ORDER BY id ASC LIMIT 1")
                row = cursor.fetchone()
                if row:
                    task = {"db_id": row[0], "msg_id": row[1], "data": json.loads(row[2])}
                    # 標記為處理中，防止併發衝突
                    conn.execute("UPDATE tasks SET status = 'processing' WHERE id = ?", (task["db_id"],))
                    conn.commit()

            if task:
                success = False
                # 判斷訊息種類
                if task["data"]["type"] == "text":
                    # 文字訊息直接發送
                    resp = requests.post(
                        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id": TELEGRAM_CHAT_ID, "text": task["data"]["text"]},
                        timeout=20
                    )
                    success = (resp.status_code == 200)
                else:
                    # 檔案訊息執行串流下載與轉傳
                    success = download_and_forward(task["data"])

                with sqlite3.connect(DB_FILE) as conn:
                    if success:
                        # 成功則移除任務
                        conn.execute("DELETE FROM tasks WHERE id = ?", (task["db_id"],))
                    else:
                        # 失敗則退回 pending 等待下次輪詢，並增加重試計計數
                        conn.execute("UPDATE tasks SET status = 'pending', retry_count = retry_count + 1 WHERE id = ?", (task["db_id"],))
                    conn.commit()
                
                # 強制休息 1.2 秒，確保每分鐘頻率不超標
                time.sleep(1.2)
            else:
                # 沒任務，休息 2 秒再檢查
                time.sleep(2)
        except Exception as e:
            print(f"[Worker 迴圈錯誤] {e}")
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
        if event.get("type") != "message": continue
        
        msg = event.get("message", {})
        msg_id, msg_type = msg.get("id"), msg.get("type")

        # 封裝任務資料
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

        # 存入資料庫持久化排隊 (包含防重複邏輯)
        save_task_safely(msg_id, task_data)

    # 立即回覆 LINE OK，處理邏輯交給背景 Worker
    return "OK", 200

@app.route("/status")
def status():
    """查看目前佇列健康狀況"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
            counts = dict(cursor.fetchall())
        return jsonify({
            "waiting_in_queue": counts.get("pending", 0),
            "processing_right_now": counts.get("processing", 0)
        })
    except:
        return jsonify({"status": "busy"}), 500

if __name__ == "__main__":
    init_db()
    # 啟動唯一的背景轉發執行緒
    threading.Thread(target=worker, daemon=True).start()
    
    # 監聽通訊埠
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
