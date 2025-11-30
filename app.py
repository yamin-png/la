import os
import json
import time
import random
import asyncio
import threading
from datetime import datetime, timezone, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, error, ChatMember
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode, ChatMemberStatus
import logging
import sys
import re
import configparser
import requests

# --- Configuration ---
CONFIG_FILE = 'config.txt'

def load_config():
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'w') as f:
            f.write("[Settings]\nPHPSESSID=rpimjduka5o0bqp2hb3k1lrcp8\n")
        print(f"Created {CONFIG_FILE}. Please configure it.")
        
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config['Settings']

try:
    config = load_config()
except (FileNotFoundError, KeyError, ValueError) as e:
    print(f"Configuration Error: {e}")
    sys.exit(1)
    
# --- Bot & Channel Credentials ---
TELEGRAM_BOT_TOKEN = "7811577720:AAGNoS9KEaziHpllsdYu1v2pGqQU7TVqJGE"
GROUP_ID = -1003009605120
PAYMENT_CHANNEL_ID = -1003184589906
REQUIRED_CHANNEL_ID = -1002141326763  # Rule 1: Forced Channel Subscription
ADMIN_ID = 5473188537

# --- Links & Text ---
GROUP_LINK = "https://t.me/guarantesms"
NUMBER_BOT_LINK = "https://t.me/ToolExprole_bot"
UPDATE_GROUP_LINK = "https://chat.whatsapp.com/IR1iW9eePp3Kfx44sKO6u9"
ADMIN_USERNAME = "@guarantesmss" 

# --- Economics & Rules ---
SMS_AMOUNT = 0.005          # $0.005 per OTP
REFERRAL_PERCENT = 0.05     # 5% Commission
BDT_RATE = 125              # 1 USD = 125 BDT
NUMBER_TIMEOUT_MINUTES = 10 

# Withdrawal Limits (USD)
MIN_WITHDRAW_BINANCE = 1.0
MIN_WITHDRAW_BKASH_BDT = 100 
MIN_WITHDRAW_BKASH_USD = MIN_WITHDRAW_BKASH_BDT / BDT_RATE

# --- Panel Configuration ---
PANEL_BASE_URL = "http://51.89.99.105/NumberPanel"
PHPSESSID = config.get('PHPSESSID', 'rpimjduka5o0bqp2hb3k1lrcp8')

# --- File Paths ---
USERS_FILE = 'users.json'
SMS_CACHE_FILE = 'sms.txt'
SENT_SMS_FILE = 'sent_sms.json'
NUMBERS_FILE = 'numbers.txt' 

# --- Global State ---
shutdown_event = asyncio.Event()
MESSAGE_QUEUE = asyncio.Queue()
FILE_LOCK = threading.Lock() 
USERS_CACHE = {} 

# --- Logging ---
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format='%(asctime)s %(levelname)s %(message)s'
)
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)

# --- Data Persistence ---

def load_json_data(filepath, default_data):
    if not os.path.exists(filepath):
        return default_data
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return default_data

def save_json_data(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def load_users_cache():
    global USERS_CACHE
    USERS_CACHE = load_json_data(USERS_FILE, {})
    # Migration: Ensure all fields exist
    current_time = time.time()
    for uid in USERS_CACHE:
        user = USERS_CACHE[uid]
        if 'active_numbers' not in user: user['active_numbers'] = []
        if 'referrer_id' not in user: user['referrer_id'] = None
        if 'last_seen' not in user: user['last_seen'] = current_time
        if 'balance' not in user: user['balance'] = 0.0
        if 'username' not in user: user['username'] = "Unknown"
        if 'first_name' not in user: user['first_name'] = "User"
            
    logging.info(f"Loaded {len(USERS_CACHE)} users.")

def background_save_users():
    try:
        save_json_data(USERS_FILE, USERS_CACHE)
    except Exception as e:
        logging.error(f"Failed to save users: {e}")

def load_sent_sms_keys():
    return set(load_json_data(SENT_SMS_FILE, []))

def save_sent_sms_keys(keys):
    save_json_data(SENT_SMS_FILE, list(keys))

def load_numbers_set(filepath):
    if not os.path.exists(filepath): return set()
    with FILE_LOCK:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return set(line.strip() for line in f if line.strip())
        except: return set()

def save_numbers_set(filepath, numbers_set):
    with FILE_LOCK:
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('\n'.join(sorted(list(numbers_set))) + "\n")
        except Exception as e:
            logging.error(f"Failed to save numbers: {e}")

# --- Helper Functions & Country Data ---

COUNTRY_PREFIXES = {
    "1": ("United States", "🇺🇸"), "7": ("Russia", "🇷🇺"), "20": ("Egypt", "🇪🇬"), "27": ("South Africa", "🇿🇦"),
    "30": ("Greece", "🇬🇷"), "31": ("Netherlands", "🇳🇱"), "32": ("Belgium", "🇧🇪"), "33": ("France", "🇫🇷"),
    "34": ("Spain", "🇪🇸"), "36": ("Hungary", "🇭🇺"), "39": ("Italy", "🇮🇹"), "40": ("Romania", "🇷🇴"),
    "41": ("Switzerland", "🇨🇭"), "43": ("Austria", "🇦🇹"), "44": ("United Kingdom", "🇬🇧"), "45": ("Denmark", "🇩🇰"),
    "46": ("Sweden", "🇸🇪"), "47": ("Norway", "🇳🇴"), "48": ("Poland", "🇵🇱"), "49": ("Germany", "🇩🇪"),
    "51": ("Peru", "🇵🇪"), "52": ("Mexico", "🇲🇽"), "53": ("Cuba", "🇨🇺"), "54": ("Argentina", "🇦🇷"),
    "55": ("Brazil", "🇧🇷"), "56": ("Chile", "🇨🇱"), "57": ("Colombia", "🇨🇴"), "58": ("Venezuela", "🇻🇪"),
    "60": ("Malaysia", "🇲🇾"), "61": ("Australia", "🇦🇺"), "62": ("Indonesia", "🇮🇩"), "63": ("Philippines", "🇵🇭"),
    "64": ("New Zealand", "🇳🇿"), "65": ("Singapore", "🇸🇬"), "66": ("Thailand", "🇹🇭"), "81": ("Japan", "🇯🇵"),
    "82": ("South Korea", "🇰🇷"), "84": ("Vietnam", "🇻🇳"), "86": ("China", "🇨🇳"), "90": ("Turkey", "🇹🇷"),
    "91": ("India", "🇮🇳"), "92": ("Pakistan", "🇵🇰"), "93": ("Afghanistan", "🇦🇫"), "94": ("Sri Lanka", "🇱🇰"),
    "95": ("Myanmar", "🇲🇲"), "98": ("Iran", "🇮🇷"), "212": ("Morocco", "🇲🇦"), "213": ("Algeria", "🇩🇿"),
    "216": ("Tunisia", "🇹🇳"), "218": ("Libya", "🇱🇾"), "220": ("Gambia", "🇬🇲"), "221": ("Senegal", "🇸🇳"),
    "222": ("Mauritania", "🇲🇷"), "223": ("Mali", "🇲🇱"), "224": ("Guinea", "🇬🇳"), "225": ("Ivory Coast", "🇨🇮"),
    "226": ("Burkina Faso", "🇧🇫"), "227": ("Niger", "🇳🇪"), "228": ("Togo", "🇹🇬"), "229": ("Benin", "🇧🇯"),
    "230": ("Mauritius", "🇲🇺"), "231": ("Liberia", "🇱🇷"), "232": ("Sierra Leone", "🇸🇱"), "233": ("Ghana", "🇬🇭"),
    "234": ("Nigeria", "🇳🇬"), "235": ("Chad", "🇹🇩"), "236": ("Central African Republic", "🇨🇫"), "237": ("Cameroon", "🇨🇲"),
    "238": ("Cape Verde", "🇨🇻"), "239": ("Sao Tome and Principe", "🇸🇹"), "240": ("Equatorial Guinea", "🇬🇶"), "241": ("Gabon", "🇬🇦"),
    "242": ("Congo", "🇨🇬"), "243": ("Congo", "🇨🇩"), "244": ("Angola", "🇦🇴"), "245": ("Guinea-Bissau", "🇬🇼"),
    "246": ("British Indian Ocean Territory", "🇮🇴"), "248": ("Seychelles", "🇸🇨"), "249": ("Sudan", "🇸🇩"), "250": ("Rwanda", "🇷🇼"),
    "251": ("Ethiopia", "🇪🇹"), "252": ("Somalia", "🇸🇴"), "253": ("Djibouti", "🇩🇯"), "254": ("Kenya", "🇰🇪"),
    "255": ("Tanzania", "🇹🇿"), "256": ("Uganda", "🇺🇬"), "257": ("Burundi", "🇧🇮"), "258": ("Mozambique", "🇲🇿"),
    "260": ("Zambia", "🇿🇲"), "261": ("Madagascar", "🇲🇬"), "262": ("Reunion", "🇷🇪"), "263": ("Zimbabwe", "🇿🇼"),
    "264": ("Namibia", "🇳🇦"), "265": ("Malawi", "🇲🇼"), "266": ("Lesotho", "🇱🇸"), "267": ("Botswana", "🇧🇼"),
    "268": ("Eswatini", "🇸🇿"), "269": ("Comoros", "🇰🇲"), "290": ("Saint Helena", "🇸🇭"), "291": ("Eritrea", "🇪🇹"),
    "297": ("Aruba", "🇦🇼"), "298": ("Faroe Islands", "🇫🇴"), "299": ("Greenland", "🇬🇱"), "350": ("Gibraltar", "🇬🇮"),
    "351": ("Portugal", "🇵🇹"), "352": ("Luxembourg", "🇱🇺"), "353": ("Ireland", "🇮🇪"), "354": ("Iceland", "🇮🇸"),
    "355": ("Albania", "🇦🇱"), "356": ("Malta", "🇲🇹"), "357": ("Cyprus", "🇨🇾"), "358": ("Finland", "🇫🇮"),
    "359": ("Bulgaria", "🇧🇬"), "370": ("Lithuania", "🇱🇹"), "371": ("Latvia", "🇱🇻"), "372": ("Estonia", "🇪🇪"),
    "373": ("Moldova", "🇲🇩"), "374": ("Armenia", "🇦🇲"), "375": ("Belarus", "🇧🇾"), "376": ("Andorra", "🇦🇩"),
    "377": ("Monaco", "🇲🇨"), "378": ("San Marino", "🇸🇲"), "380": ("Ukraine", "🇺🇦"), "381": ("Serbia", "🇷🇸"),
    "382": ("Montenegro", "🇲🇪"), "383": ("Kosovo", "🇽🇰"), "385": ("Croatia", "🇭🇷"), "386": ("Slovenia", "🇸🇮"),
    "387": ("Bosnia and Herzegovina", "🇧🇦"), "389": ("North Macedonia", "🇲🇰"), "420": ("Czech Republic", "🇨🇿"),
    "421": ("Slovakia", "🇸🇰"), "423": ("Liechtenstein", "🇱🇮"), "500": ("Falkland Islands", "🇫🇰"),
    "501": ("Belize", "🇧🇿"), "502": ("Guatemala", "🇬🇹"), "503": ("El Salvador", "🇸🇻"), "504": ("Honduras", "🇭🇳"),
    "505": ("Nicaragua", "🇳🇮"), "506": ("Costa Rica", "🇨🇷"), "507": ("Panama", "🇵🇦"), "508": ("Saint Pierre and Miquelon", "🇵🇲"),
    "509": ("Haiti", "🇭🇹"), "590": ("Guadeloupe", "🇬🇵"), "591": ("Bolivia", "🇧🇴"), "592": ("Guyana", "🇬🇾"),
    "593": ("Ecuador", "🇪🇨"), "594": ("French Guiana", "🇬🇫"), "595": ("Paraguay", "🇵🇾"), "596": ("Martinique", "🇲🇶"),
    "597": ("Suriname", "🇸🇷"), "598": ("Uruguay", "🇺🇾"), "599": ("Netherlands Antilles", "🇳🇱"), "670": ("Timor-Leste", "🇹🇱"),
    "672": ("Australian External Territories", "🇦🇺"), "673": ("Brunei", "🇧🇳"), "674": ("Nauru", "🇳🇷"),
    "675": ("Papua New Guinea", "🇵🇬"), "676": ("Tonga", "🇹🇴"), "677": ("Solomon Islands", "🇸🇧"), "678": ("Vanuatu", "🇻🇺"),
    "679": ("Fiji", "🇫🇯"), "680": ("Palau", "🇵🇼"), "681": ("Wallis and Futuna", "🇼🇫"), "682": ("Cook Islands", "🇨🇰"),
    "683": ("Niue", "🇳🇺"), "684": ("American Samoa", "🇦🇸"), "685": ("Samoa", "🇼🇸"), "686": ("Kiribati", "🇰🇮"),
    "687": ("New Caledonia", "🇳🇨"), "688": ("Tuvalu", "🇹🇻"), "689": ("French Polynesia", "🇵🇫"), "690": ("Tokelau", "🇹🇰"),
    "691": ("Micronesia", "🇫🇲"), "692": ("Marshall Islands", "🇲🇭"), "850": ("North Korea", "🇰🇵"), "852": ("Hong Kong", "🇭🇰"),
    "853": ("Macau", "🇲🇴"), "855": ("Cambodia", "🇰🇭"), "856": ("Laos", "🇱🇦"), "880": ("Bangladesh", "🇧🇩"),
    "886": ("Taiwan", "🇹🇼"), "960": ("Maldives", "🇲🇻"), "961": ("Lebanon", "🇱🇧"), "962": ("Jordan", "🇯🇴"),
    "963": ("Syria", "🇸🇾"), "964": ("Iraq", "🇮🇶"), "965": ("Kuwait", "🇰🇼"), "966": ("Saudi Arabia", "🇸🇦"),
    "967": ("Yemen", "🇾🇪"), "968": ("Oman", "🇴🇲"), "970": ("Palestine", "🇵🇸"), "971": ("United Arab Emirates", "🇦🇪"),
    "972": ("Israel", "🇮🇱"), "973": ("Bahrain", "🇧🇭"), "974": ("Qatar", "🇶🇦"), "975": ("Bhutan", "🇧🇹"),
    "976": ("Mongolia", "🇲🇳"), "977": ("Nepal", "🇳🇵"), "992": ("Tajikistan", "🇹🇯"), "993": ("Turkmenistan", "🇹🇲"),
    "994": ("Azerbaijan", "🇦🇿"), "995": ("Georgia", "🇬🇪"), "996": ("Kyrgyzstan", "🇰🇬"), "998": ("Uzbekistan", "🇺🇿"),
}

def detect_country_from_phone(phone):
    if not phone: return "Unknown", "🌍"
    phone_str = str(phone).replace("+", "").replace(" ", "").replace("-", "").strip()
    # Check largest prefixes first
    for length in [4, 3, 2, 1]:
        prefix = phone_str[:length]
        if prefix in COUNTRY_PREFIXES:
            return COUNTRY_PREFIXES[prefix]
    return "Unknown", "🌍"

def get_number_from_pool(country_name):
    target_country = str(country_name).strip().lower()
    regular_numbers = load_numbers_set(NUMBERS_FILE)
    match = None
    for number in regular_numbers:
        detected_name, _ = detect_country_from_phone(number)
        if detected_name.lower() == target_country:
            match = number
            break
    if match:
        regular_numbers.remove(match)
        save_numbers_set(NUMBERS_FILE, regular_numbers)
        return match
    return None

def remove_number_from_pool(number):
    number = str(number).strip()
    regular_numbers = load_numbers_set(NUMBERS_FILE)
    if number in regular_numbers:
        regular_numbers.remove(number)
        save_numbers_set(NUMBERS_FILE, regular_numbers)

def add_numbers_to_file(number_list):
    if not number_list: return
    current_numbers = load_numbers_set(NUMBERS_FILE)
    added_count = 0
    for num in number_list:
        clean_num = num.strip()
        if clean_num.isdigit() and len(clean_num) > 5 and clean_num not in current_numbers:
            current_numbers.add(clean_num)
            added_count += 1
    if added_count > 0:
        save_numbers_set(NUMBERS_FILE, current_numbers)

def hide_number(number):
    if len(str(number)) > 7:
        num_str = str(number)
        return f"{num_str[:3]}XXXX{num_str[-4:]}"
    return number

def html_escape(text):
    return str(text).replace('<', '&lt;').replace('>', '&gt;')

def extract_otp_from_text(text):
    if not text: return "N/A"
    patterns = [
        r'code\s*(\d{3}\s+\d{3})', r'(\d{3}\s+\d{3})', r'(\d{3}-\d{3})', 
        r'G-(\d{6})', r'code\s*is\s*(\d+)', r'code:\s*(\d+)', r'pin[:\s]*(\d+)', 
        r'\b(\d{4,8})\b'
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            otp = match.group(1).replace('-', '').replace(' ', '')
            return otp
    return "N/A"

# --- Panel Manager ---
class NewPanelSmsManager:
    def fetch_sms_from_api(self):
        today = datetime.now().strftime("%Y-%m-%d")
        url = f"{PANEL_BASE_URL}/agent/res/data_smscdr.php?fdate1={today}+00:00:00&fdate2={today}+23:59:59&iDisplayLength=500"
        headers = {
            "accept": "application/json", "x-requested-with": "XMLHttpRequest", 
            "cookie": f"PHPSESSID={PHPSESSID}", "referer": f"{PANEL_BASE_URL}/agent/SMSDashboard",
            "user-agent": "Mozilla/5.0"
        }
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    return data.get('aaData', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                except: pass
            return []
        except: return []

    def scrape_and_save_all_sms(self):
        sms_data = self.fetch_sms_from_api()
        if not sms_data: return
        sms_list = []
        for row in sms_data:
            try:
                if len(row) >= 6:
                    phone = str(row[2]) if row[2] else "N/A"
                    msg = str(row[5]) if row[5] else "N/A"
                    if phone and msg:
                        sms_list.append({
                            'country': str(row[1]).split()[0] if row[1] else "Unknown",
                            'provider': str(row[3]) if row[3] else "Service",
                            'message': msg, 'phone': phone
                        })
            except: pass
        with open(SMS_CACHE_FILE, 'w', encoding='utf-8') as f:
            for sms in sms_list: f.write(json.dumps(sms) + "\n")

# --- Keyboards ---

def get_main_menu_keyboard():
    # Rule 5: Updated Menu Structure
    keyboard = [
        [KeyboardButton("🎁 Get Number"), KeyboardButton("💰 Balance")],
        [KeyboardButton("💸 Withdraw"), KeyboardButton("👤 Account")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_user_country_keyboard():
    all_numbers = load_numbers_set(NUMBERS_FILE)
    counts = {}
    for number in all_numbers:
        name, flag = detect_country_from_phone(number)
        if name != "Unknown":
            if name not in counts: counts[name] = {'flag': flag, 'count': 0}
            counts[name]['count'] += 1
    
    available = sorted([(d['flag'], n, d['count']) for n, d in counts.items()], key=lambda x: x[1])
    if not available:
        return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]), True
    
    keyboard = []
    for i in range(0, len(available), 2):
        row = []
        for j in range(2):
            if i + j < len(available):
                flag, name, count = available[i + j]
                row.append(InlineKeyboardButton(f"{flag} {name} ({count})", callback_data=f"user_country_{name}"))
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard), False

async def check_subscription(user_id, bot):
    # Rule 1: Forced Channel Subscription
    if str(user_id) == str(ADMIN_ID): return True
    try:
        member = await bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id)
        if member.status in [ChatMemberStatus.LEFT, ChatMemberStatus.BANNED]:
            return False
        return True
    except Exception:
        # If bot is not admin or channel private, assume false or handle gracefully
        return False 

async def ask_subscription(update, context):
    # This URL should be the invite link for the channel -1002141326763
    channel_invite_link = "https://t.me/guarantesms" 
    
    keyboard = [
        [InlineKeyboardButton("📢 Join Channel", url=channel_invite_link)],
        [InlineKeyboardButton("✅ Check Joined", callback_data='check_sub')]
    ]
    
    msg = "<b>⚠️ Access Denied</b>\n\nYou must join our channel to use this bot."
    
    if update.callback_query:
        await update.callback_query.answer("Join channel first!", show_alert=True)
        # Avoid editing if text is same, just send new if needed or simple answer
        try:
            await update.callback_query.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
        except: pass
    else:
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

# --- Tasks ---

async def delayed_broadcast_task(app, summary_text):
    # Rule 3: Auto Broadcast after 10 mins (600 seconds)
    # This runs in background, doesn't freeze bot
    await asyncio.sleep(600)
    try:
        msg = (
            f"<b>🔔 New Numbers Added!</b>\n\n"
            f"{summary_text}\n\n"
            f"<b>Start work. Gain more!</b>"
        )
        await app.bot.send_message(chat_id=GROUP_ID, text=msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Broadcast failed: {e}")

async def rate_limited_sender_task(app):
    while not shutdown_event.is_set():
        try:
            msg = await MESSAGE_QUEUE.get()
            try:
                await app.bot.send_message(
                    chat_id=msg['chat_id'], text=msg['text'], 
                    parse_mode=msg['parse_mode'], reply_markup=msg.get('reply_markup')
                )
            except Exception: pass
            MESSAGE_QUEUE.task_done()
            await asyncio.sleep(0.05)
        except asyncio.CancelledError: break

async def inactivity_checker_task(app):
    # Rule 4: Inactivity Reminder (12 hours)
    while not shutdown_event.is_set():
        try:
            now = time.time()
            cutoff = now - (12 * 3600) # 12 hours ago
            
            for uid, data in USERS_CACHE.items():
                last_seen = data.get('last_seen', 0)
                last_reminded = data.get('last_inactivity_reminder', 0)
                
                # If inactive > 12h AND not reminded in last 24h
                if last_seen < cutoff and (now - last_reminded > 24 * 3600):
                    msg = (
                        "দুঃখিত আপনি কেন আমাদের বট টা ব্যবহার করছেন না ? "
                        "কোড রিসিভ হচ্ছে না ? আপনি অ্যাডমিন এর সাথে যোগাযোগ করতে পারেন - @guarantesmss"
                    )
                    try:
                        await app.bot.send_message(chat_id=uid, text=msg)
                        data['last_inactivity_reminder'] = now
                    except: pass 
            
            await asyncio.to_thread(background_save_users)
        except Exception as e:
            logging.error(f"Inactivity task error: {e}")
        await asyncio.sleep(3600) # Check every hour

async def background_number_cleanup_task(app):
    while not shutdown_event.is_set():
        try:
            now = time.time()
            timeout = NUMBER_TIMEOUT_MINUTES * 60
            users_save = False
            return_pool = set()
            
            for uid, user in USERS_CACHE.items():
                active = user.get('active_numbers', [])[:]
                new_active = []
                for num_data in active:
                    if now - num_data.get('claimed_time', 0) < timeout:
                        new_active.append(num_data)
                    else:
                        return_pool.add(num_data['number'])
                        users_save = True
                user['active_numbers'] = new_active
            
            if return_pool:
                pool = load_numbers_set(NUMBERS_FILE)
                pool.update(return_pool)
                await asyncio.to_thread(save_numbers_set, NUMBERS_FILE, pool)
            if users_save:
                await asyncio.to_thread(background_save_users)
        except Exception: pass
        await asyncio.sleep(60)

async def sms_watcher_task(app):
    manager = NewPanelSmsManager()
    sent_keys = load_sent_sms_keys()
    
    while not shutdown_event.is_set():
        try:
            await asyncio.to_thread(manager.scrape_and_save_all_sms)
            if not os.path.exists(SMS_CACHE_FILE):
                await asyncio.sleep(15)
                continue
            
            phone_map = {}
            for uid, udata in USERS_CACHE.items():
                for num in udata.get('active_numbers', []):
                    phone_map[num['number']] = uid
            
            dirty = False
            with open(SMS_CACHE_FILE, 'r') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        phone = data['phone']
                        msg_text = data['message']
                        otp = extract_otp_from_text(msg_text)
                        
                        if otp == "N/A": continue
                        key = f"{phone}|{otp}"
                        if key in sent_keys: continue
                        
                        owner_id = phone_map.get(phone)
                        name, flag = detect_country_from_phone(phone)
                        
                        # Rule 2: Group Message Footer Update
                        # "{user’s first name} earned X (~X Bdt) . Be active"
                        owner_name = "{user}"
                        if owner_id and owner_id in USERS_CACHE:
                            owner_name = html_escape(USERS_CACHE[owner_id].get('first_name', 'User'))
                            
                        bdt_earn = SMS_AMOUNT * BDT_RATE
                        footer = f"{owner_name} earned ${SMS_AMOUNT} (~{bdt_earn:.2f} Bdt) . Be active"
                        
                        group_msg = (
                            f"<b>🔔 New OTP Received!</b> ✨\n\n"
                            f"<b>📞 Number:</b> <code>{hide_number(phone)}</code>\n"
                            f"<b>🌍 Country:</b> {html_escape(name)} {flag}\n"
                            f"<b>🆔 Service:</b> <code>{html_escape(data.get('provider','Service'))}</code>\n"
                            f"<b>🔑 Code:</b> <code>{otp}</code>\n"
                            f"<i>📝 Message:</i>\n<blockquote>{html_escape(msg_text)}</blockquote>\n\n"
                            f"<i>{footer}</i>"
                        )
                        kb = InlineKeyboardMarkup([
                            [InlineKeyboardButton("number bot", url=NUMBER_BOT_LINK),
                             InlineKeyboardButton("update group", url=UPDATE_GROUP_LINK)]
                        ])
                        await MESSAGE_QUEUE.put({'chat_id': GROUP_ID, 'text': group_msg, 'parse_mode': ParseMode.HTML, 'reply_markup': kb})
                        
                        if owner_id and owner_id in USERS_CACHE:
                            user = USERS_CACHE[owner_id]
                            user['balance'] += SMS_AMOUNT
                            
                            # Rule 6: Referral Commission (5% per SMS)
                            referrer_id = user.get('referrer_id')
                            if referrer_id and referrer_id in USERS_CACHE:
                                comm = SMS_AMOUNT * REFERRAL_PERCENT
                                USERS_CACHE[referrer_id]['balance'] += comm
                                try:
                                    await app.bot.send_message(
                                        chat_id=referrer_id, 
                                        text=f"<b>💸 Referral Bonus!</b>\nYou earned ${comm:.4f} from a referral SMS.",
                                        parse_mode=ParseMode.HTML
                                    )
                                except: pass
                            
                            # Cleanup: Remove number after receiving code
                            user['active_numbers'] = [n for n in user['active_numbers'] if n['number'] != phone]
                            asyncio.to_thread(remove_number_from_pool, phone)
                            
                            user_msg = (
                                f"<b>🔔 New OTP Received!</b> ✨\n\n"
                                f"<b>📞 Number:</b> <code>{phone}</code>\n"
                                f"<b>🔑 Code:</b> <code>{otp}</code>\n"
                                f"<b>💰 Earned: ${SMS_AMOUNT}</b>"
                            )
                            await MESSAGE_QUEUE.put({'chat_id': owner_id, 'text': user_msg, 'parse_mode': ParseMode.HTML})
                            dirty = True
                        
                        sent_keys.add(key)
                    except: pass
            
            if dirty: await asyncio.to_thread(background_save_users)
            save_sent_sms_keys(sent_keys)
        except Exception: pass
        await asyncio.sleep(15)

# --- Commands & Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = str(user.id)
    
    # Rule 10: Capture data regardless of start
    if uid not in USERS_CACHE:
        USERS_CACHE[uid] = {
            "username": user.username, "first_name": user.first_name,
            "active_numbers": [], "balance": 0.0, "last_seen": time.time(),
            "referrer_id": None
        }
    
    # Update Info
    USERS_CACHE[uid]['first_name'] = user.first_name
    USERS_CACHE[uid]['username'] = user.username
    USERS_CACHE[uid]['last_seen'] = time.time()
    
    # Rule 4: Retroactive Referral (No Override)
    args = context.args
    if args and len(args) > 0:
        possible_ref = args[0]
        current_ref = USERS_CACHE[uid].get('referrer_id')
        if not current_ref and possible_ref in USERS_CACHE and possible_ref != uid:
            USERS_CACHE[uid]['referrer_id'] = possible_ref
            logging.info(f"User {uid} retroactively referred by {possible_ref}")
    
    await asyncio.to_thread(background_save_users)
    
    # Rule 1: Check Subscription
    if not await check_subscription(user.id, context.bot):
        await ask_subscription(update, context)
        return

    welcome = "<b>👋 Welcome!</b>\n\n<i>Select an option from the menu below:</i>"
    await update.message.reply_text(welcome, reply_markup=get_main_menu_keyboard(), parse_mode=ParseMode.HTML)

async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != str(ADMIN_ID): return
    context.user_data['state'] = 'ADDING_NUMBER'
    await update.message.reply_text("<b>📞 Send numbers to add:</b>", parse_mode=ParseMode.HTML)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return
    user = update.effective_user
    uid = str(user.id)
    text = update.message.text
    
    # Rule 10: Capture User Data & Update Info on ANY message
    if uid not in USERS_CACHE:
        USERS_CACHE[uid] = {
            "username": user.username, "first_name": user.first_name,
            "active_numbers": [], "balance": 0.0, "referrer_id": None
        }
    
    # Rule 7: Auto-Update User File
    USERS_CACHE[uid]['last_seen'] = time.time()
    USERS_CACHE[uid]['username'] = user.username 
    USERS_CACHE[uid]['first_name'] = user.first_name

    # Rule 1: Check Subscription
    if not await check_subscription(user.id, context.bot):
        await ask_subscription(update, context)
        return

    state = context.user_data.get('state')

    # --- Admin Logic ---
    if state == 'ADDING_NUMBER' and uid == str(ADMIN_ID):
        numbers = [n.strip() for n in text.split('\n') if n.strip().isdigit()]
        if numbers:
            await asyncio.to_thread(add_numbers_to_file, numbers)
            
            # Prepare Broadcast Summary
            country_counts = {}
            for num in numbers:
                name, _ = detect_country_from_phone(num)
                country_counts[name] = country_counts.get(name, 0) + 1
            
            summary = "\n".join([f"{c} ({n})" for c, n in country_counts.items()])
            
            # Rule 3: Async broadcast after 10 minutes
            asyncio.create_task(delayed_broadcast_task(context.application, summary))
            
            context.user_data['state'] = None
            await update.message.reply_text(f"✅ Added {len(numbers)} numbers. Auto-broadcast scheduled in 10 mins.")
        else:
            await update.message.reply_text("❌ No valid numbers found.")
        return

    # --- Withdrawal Logic ---
    if state == 'AWAITING_WITHDRAWAL_AMOUNT':
        try:
            amount = float(text.strip())
            method = context.user_data.get('withdraw_method')
            
            # Logic Check
            min_usd = MIN_WITHDRAW_BINANCE if method == 'Binance' else MIN_WITHDRAW_BKASH_USD
            if amount < min_usd:
                await update.message.reply_text(f"❌ Minimum for {method} is ${min_usd:.2f}")
                return
            
            if amount > USERS_CACHE[uid].get('balance', 0):
                await update.message.reply_text("❌ Insufficient balance.")
                return

            context.user_data['withdraw_amount'] = amount
            context.user_data['state'] = f'AWAITING_WITHDRAWAL_ACCOUNT_{method}'
            await update.message.reply_text(f"<b>Enter your {method} wallet/number:</b>", parse_mode=ParseMode.HTML)
            return
            
        except ValueError:
            await update.message.reply_text("❌ Invalid number.")
            return

    if state and state.startswith('AWAITING_WITHDRAWAL_ACCOUNT_'):
        account = text.strip()
        method = state.split('_')[-1]
        amount = context.user_data.get('withdraw_amount')
        
        user_bal = USERS_CACHE[uid]['balance']
        if user_bal < amount:
            await update.message.reply_text("❌ Insufficient balance.")
            context.user_data['state'] = None
            return
            
        # Deduct Balance
        USERS_CACHE[uid]['balance'] -= amount
        await asyncio.to_thread(background_save_users)
        
        bdt_final = int(amount * BDT_RATE)
        fee_msg = ""
        
        # Rule 9: Fee Logic for Bkash > 50 BDT (Extra 5 BDT fee shown to user/admin)
        if method == 'Bkash' and bdt_final > 50:
            bdt_final -= 5
            fee_msg = "(5 BDT Fee Deducted)"
        
        msg = (
            f"<b>💸 Withdrawal Request</b>\n"
            f"User: {html_escape(user.first_name)} (ID: {uid})\n"
            f"Amount: <b>${amount:.3f}</b>\n"
            f"Payout: <b>{bdt_final} BDT</b> {fee_msg}\n"
            f"Method: {method}\n"
            f"Account: <code>{account}</code>"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Approve", callback_data=f'admin_approve_{uid}_{amount}'),
             InlineKeyboardButton("Decline", callback_data=f'admin_decline_{uid}_{amount}')]
        ])
        await context.bot.send_message(chat_id=PAYMENT_CHANNEL_ID, text=msg, reply_markup=kb, parse_mode=ParseMode.HTML)
        await update.message.reply_text("✅ Request Submitted.")
        context.user_data['state'] = None
        return

    # --- Balance Transfer Logic (Rule 6) ---
    if state == 'TRANSFER_GET_USER':
        target_input = text.strip().replace('@', '').lower()
        target_id = None
        target_name = None
        target_username = None
        
        # Search for user
        for u, d in USERS_CACHE.items():
            u_name = d.get('username', '').lower() if d.get('username') else ""
            if u == target_input or u_name == target_input:
                target_id = u
                target_name = d.get('first_name', 'User')
                target_username = d.get('username', 'NoUsername')
                break
        
        if not target_id or target_id == uid:
            await update.message.reply_text("❌ User not found or invalid.")
            context.user_data['state'] = None
            return
            
        context.user_data['transfer_target'] = target_id
        context.user_data['transfer_name'] = target_name
        context.user_data['transfer_username'] = target_username
        context.user_data['state'] = 'TRANSFER_GET_AMOUNT'
        
        msg = (
            f"<b>✅ User Found!</b>\n"
            f"Name: {html_escape(target_name)}\n"
            f"Username: @{html_escape(target_username)}\n\n"
            f"<b>Enter amount to transfer ($):</b>"
        )
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        return

    if state == 'TRANSFER_GET_AMOUNT':
        try:
            amount = float(text)
            if amount <= 0 or amount > USERS_CACHE[uid]['balance']:
                await update.message.reply_text("❌ Invalid amount or insufficient balance.")
                return
            
            target_name = context.user_data['transfer_name']
            target_username = context.user_data['transfer_username']
            target_id = context.user_data['transfer_target']
            
            # Verification Confirmation
            msg = (
                f"<b>🔄 Confirm Transfer?</b>\n\n"
                f"To: <b>{html_escape(target_name)}</b> (@{html_escape(target_username)})\n"
                f"Amount: <b>${amount:.4f}</b>"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_transfer_{target_id}_{amount}"),
                 InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]
            ])
            await update.message.reply_text(msg, reply_markup=kb, parse_mode=ParseMode.HTML)
            context.user_data['state'] = None
        except:
            await update.message.reply_text("❌ Invalid amount.")
        return

    # --- Standard Menus ---
    if text == "🎁 Get Number":
        # Rule 11: Clean Inbox (Delete user msg)
        try: await context.bot.delete_message(chat_id=uid, message_id=update.message.message_id)
        except: pass
        
        kb, empty = get_user_country_keyboard()
        txt = "<b>🌍 Select Country:</b>" if not empty else "<b>😔 No numbers.</b>"
        await update.message.reply_text(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
        
    elif text == "👤 Account":
        # Rule 5: Account only shows Referral Link
        bot_username = context.bot.username
        link = f"https://t.me/{bot_username}?start={uid}"
        msg = f"<b>🔗 Your Referral Link:</b>\n<code>{link}</code>\n\n<i>Share to earn {int(REFERRAL_PERCENT*100)}% commission!</i>"
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

    elif text == "💰 Balance":
        # Rule 5 & 6: Balance Section with Transfer Button
        bal = USERS_CACHE[uid].get('balance', 0.0)
        bdt = int(bal * BDT_RATE)
        msg = f"<b>💰 Balance:</b> ${bal:.4f} (~{bdt} BDT)"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("💸 Balance Transfer", callback_data="start_transfer")]])
        await update.message.reply_text(msg, reply_markup=kb, parse_mode=ParseMode.HTML)

    elif text == "💸 Withdraw":
        # Rule 5 & 8: Separate Withdraw Menu
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Bkash (Min 100৳)", callback_data='withdraw_method_Bkash')],
            [InlineKeyboardButton("Binance (Min $1)", callback_data='withdraw_method_Binance')]
        ])
        await update.message.reply_text("<b>💸 Select Method:</b>", reply_markup=kb, parse_mode=ParseMode.HTML)
        
    else:
        # Fallback to start for unknown commands or resets
        await start_command(update, context)

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = str(query.from_user.id)
    data = query.data
    
    # Rule 10 & 7: Update info on interaction
    if uid in USERS_CACHE:
        USERS_CACHE[uid]['last_seen'] = time.time()
        USERS_CACHE[uid]['first_name'] = query.from_user.first_name
        USERS_CACHE[uid]['username'] = query.from_user.username

    # Rule 1: Check Subscription (Skip for admin approvals/checks)
    if data == 'check_sub':
        if await check_subscription(uid, context.bot):
            await query.answer("✅ Verified! Welcome back.")
            await query.message.delete()
            await start_command(update, context)
        else:
            await query.answer("❌ Not joined yet.", show_alert=True)
        return

    if not data.startswith('admin_'):
        if not await check_subscription(query.from_user.id, context.bot):
            await ask_subscription(update, context)
            return

    if data == 'main_menu':
        await query.message.delete()
        await start_command(update, context)
        return

    if data.startswith('user_country_'):
        country = data.replace('user_country_', '')
        user = USERS_CACHE.get(uid)
        
        # Rule 11: Clean Inbox (Delete menu)
        try: await query.message.delete()
        except: pass
        
        number = await asyncio.to_thread(get_number_from_pool, country)
        if number:
            if 'active_numbers' not in user: user['active_numbers'] = []
            
            user['active_numbers'].append({'number': number, 'claimed_time': time.time()})
            # Keep only latest number to avoid confusion/clutter? Or allow multiple? 
            # Prompt implies "get number", usually singular context.
            
            name, flag = detect_country_from_phone(number)
            msg = (
                f"<b>🎉 Number Acquired!</b>\n\n"
                f"<b>🌍 Country:</b> {flag} <i>{name}</i>\n"
                f"<b>📞 Number:</b> <code>{number}</code>\n\n"
                f"<i>⏳ Waiting for SMS...</i>"
            )
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("OTP GROUP", url=GROUP_LINK)]])
            await context.bot.send_message(chat_id=uid, text=msg, parse_mode=ParseMode.HTML, reply_markup=kb)
        else:
            await context.bot.send_message(chat_id=uid, text="<b>😔 Number unavailable.</b>", parse_mode=ParseMode.HTML)

    # --- Transfer Flow ---
    elif data == 'start_transfer':
        context.user_data['state'] = 'TRANSFER_GET_USER'
        await query.message.reply_text("<b>👤 Enter recipient Username or ID:</b>", parse_mode=ParseMode.HTML)
        await query.answer()

    elif data.startswith('confirm_transfer_'):
        parts = data.split('_')
        target_id = parts[2]
        amount = float(parts[3])
        
        if USERS_CACHE[uid]['balance'] >= amount:
            USERS_CACHE[uid]['balance'] -= amount
            if target_id in USERS_CACHE:
                USERS_CACHE[target_id]['balance'] += amount
                await asyncio.to_thread(background_save_users)
                
                await query.edit_message_text(f"✅ Transferred ${amount:.4f} successfully.")
                try:
                    await context.bot.send_message(
                        chat_id=target_id, 
                        text=f"<b>💰 Received ${amount:.4f} from {query.from_user.first_name}</b>", 
                        parse_mode=ParseMode.HTML
                    )
                except: pass
            else:
                USERS_CACHE[uid]['balance'] += amount # Refund if user somehow vanished
                await query.edit_message_text("❌ Error: User not found.")
        else:
            await query.edit_message_text("❌ Transfer Failed: Insufficient Balance.")

    # --- Withdrawal Flow ---
    elif data.startswith('withdraw_method_'):
        method = data.split('_')[2]
        context.user_data['withdraw_method'] = method
        context.user_data['state'] = 'AWAITING_WITHDRAWAL_AMOUNT'
        await query.message.reply_text(f"<b>💸 Enter amount for {method} ($):</b>", parse_mode=ParseMode.HTML)
        await query.answer()

    # --- Admin Actions ---
    elif data.startswith('admin_approve_') or data.startswith('admin_decline_'):
        if str(uid) != str(ADMIN_ID): return
        parts = data.split('_')
        action, target_id, amount = parts[1], parts[2], float(parts[3])
        
        if action == 'approve':
            await context.bot.send_message(chat_id=target_id, text=f"✅ Withdrawal of ${amount:.3f} Approved!")
            await query.edit_message_text(f"{query.message.text}\n\n✅ APPROVED", parse_mode=ParseMode.HTML)
        else:
            if target_id in USERS_CACHE:
                USERS_CACHE[target_id]['balance'] += amount
                await asyncio.to_thread(background_save_users)
            await context.bot.send_message(chat_id=target_id, text=f"❌ Withdrawal of ${amount:.3f} Declined (Refunded).")
            await query.edit_message_text(f"{query.message.text}\n\n❌ DECLINED", parse_mode=ParseMode.HTML)

if __name__ == "__main__":
    load_users_cache()
    
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("add", add_command))
    app.add_handler(CallbackQueryHandler(button_callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Bot Starting...")
    asyncio.get_event_loop().create_task(sms_watcher_task(app))
    asyncio.get_event_loop().create_task(rate_limited_sender_task(app))
    asyncio.get_event_loop().create_task(background_number_cleanup_task(app))
    asyncio.get_event_loop().create_task(inactivity_checker_task(app))
    
    app.run_polling()
