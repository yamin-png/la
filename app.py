import os
import json
import time
import random
import asyncio
import threading
from datetime import datetime, timezone, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, error
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode
import logging
import sys
import re
import configparser
import requests
import aiohttp
from bs4 import BeautifulSoup
import telegram 

# --- Configuration ---
CONFIG_FILE = 'config.txt'

def load_config():
    if not os.path.exists(CONFIG_FILE):
        print(f"CRITICAL: {CONFIG_FILE} not found! Please create it.")
        sys.exit(1)
        
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config['Settings']

try:
    config = load_config()
except (FileNotFoundError, KeyError, ValueError) as e:
    print(f"Configuration Error: {e}")
    sys.exit(1)
    
# Bot and Panel Credentials
TELEGRAM_BOT_TOKEN = "7811577720:AAGNoS9KEaziHpllsdYu1v2pGqQU7TVqJGE"
GROUP_ID = -1003009605120
PAYMENT_CHANNEL_ID = -1003184589906
ADMIN_ID = 5473188537

# Links
GROUP_LINK = "https://t.me/guaranteesms"
NUMBER_BOT_LINK = "https://t.me/ToolExprole_bot"
UPDATE_GROUP_LINK = "https://chat.whatsapp.com/IR1iW9eePp3Kfx44sKO6u9"

SMS_AMOUNT = 0.005  # $0.006 per OTP
WITHDRAWAL_LIMIT = 2.0  # Minimum $2.00 to withdraw

# New Panel Credentials
PANEL_BASE_URL = "http://51.89.99.105/NumberPanel"
PANEL_SMS_URL = f"{PANEL_BASE_URL}/agent/SMSCDRStats"
PHPSESSID = config.get('PHPSESSID', 'rpimjduka5o0bqp2hb3k1lrcp8')

# File Paths
USERS_FILE = 'users.json'
SMS_CACHE_FILE = 'sms.txt'
SENT_SMS_FILE = 'sent_sms.json'
NUMBERS_FILE = 'numbers.txt' 

# Global variables
shutdown_event = asyncio.Event()
manager_instance = None
MESSAGE_QUEUE = asyncio.Queue()
LAST_SESSION_FAILURE_NOTIFICATION = 0
FILE_LOCK = threading.Lock() # Thread-safe lock for file operations

# In-Memory Caches for Speed
USERS_CACHE = {} 

# Setup logging to TERMINAL
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format='%(asctime)s %(levelname)s %(message)s'
)

# Disable HTTP request logging
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)
logging.getLogger('httpcore').setLevel(logging.ERROR)

# Bangladesh Standard Time (BST) is UTC+6
BST_OFFSET = timedelta(hours=6)
BST_TIMEZONE = timezone(BST_OFFSET)

# ---------------------------------------------------------
# CORE UTILS
# ---------------------------------------------------------

# Mapping of Country Code -> (Country Name, Flag)
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
    "268": ("Eswatini", "🇸🇿"), "269": ("Comoros", "🇰🇲"), "290": ("Saint Helena", "🇸🇭"), "291": ("Eritrea", "🇪🇷"),
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

# Map for display names if needed (e.g., for admin menu)
COUNTRIES = {k: f"{v[1]} {v[0]}" for k, v in COUNTRY_PREFIXES.items()} 

def detect_country_from_phone(phone):
    """Detect country from phone number prefix, returns (Name, Flag)"""
    if not phone:
        return "Unknown", "🌍"
    
    phone_str = str(phone).replace("+", "").replace(" ", "").replace("-", "").strip()
    
    # Try different prefix lengths (longest first)
    for length in [4, 3, 2, 1]:
        if len(phone_str) >= length:
            prefix = phone_str[:length]
            if prefix in COUNTRY_PREFIXES:
                return COUNTRY_PREFIXES[prefix]
    
    return "Unknown", "🌍"

def get_bst_now():
    """Get current time in Bangladesh Standard Time."""
    return datetime.now(BST_TIMEZONE)

# --- Data Management ---

def load_json_data(filepath, default_data):
    if not os.path.exists(filepath):
        return default_data
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return default_data

def save_json_data(filepath, data):
    """Saves data to file. Use only in background tasks or low frequency."""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def load_users_cache():
    """Loads users into global memory cache on startup."""
    global USERS_CACHE
    USERS_CACHE = load_json_data(USERS_FILE, {})
    logging.info(f"Loaded {len(USERS_CACHE)} users into memory.")

def background_save_users():
    """Run in executor to save users data without blocking."""
    try:
        save_json_data(USERS_FILE, USERS_CACHE)
    except Exception as e:
        logging.error(f"Failed to save users data: {e}")

def load_sent_sms_keys():
    return set(load_json_data(SENT_SMS_FILE, []))

def save_sent_sms_keys(keys):
    save_json_data(SENT_SMS_FILE, list(keys))

def clean_numbers_file():
    """Cleans empty lines from numbers.txt on startup."""
    if not os.path.exists(NUMBERS_FILE): return
    with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')

# --- Number Logic ---

def get_number_from_file_for_country(country_name):
    """Gets a RANDOM number for specific country from numbers.txt file, then deletes it."""
    if not os.path.exists(NUMBERS_FILE):
        return None
    
    target_country = str(country_name).strip().lower()
    
    with FILE_LOCK:
        with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        matching_indices = []
        for i, line in enumerate(lines):
            number = line.strip()
            if not number: continue
            
            detected_name, _ = detect_country_from_phone(number)
            if detected_name.lower() == target_country:
                matching_indices.append(i)
        
        if matching_indices:
            chosen_index = random.choice(matching_indices)
            number = lines[chosen_index].strip()
            
            # Remove from file
            lines.pop(chosen_index)
            with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
                f.writelines(lines)
                
            return number

    return None

def get_available_countries_and_counts():
    """Returns list of (Flag, CountryName, Count) tuples."""
    if not os.path.exists(NUMBERS_FILE):
        return []
    
    counts = {} 
    
    # Use lock to prevent reading while writing
    with FILE_LOCK:
        with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                number = line.strip()
                if not number: continue
                
                name, flag = detect_country_from_phone(number)
                if name != "Unknown":
                    if name not in counts:
                        counts[name] = {'flag': flag, 'count': 0}
                    counts[name]['count'] += 1
    
    result = []
    for name, data in counts.items():
        result.append((data['flag'], name, data['count']))
    
    return sorted(result, key=lambda x: x[1]) 

def add_numbers_to_file(number_list):
    """Adds a list of numbers to numbers.txt file."""
    if not number_list: return
    with FILE_LOCK:
        with open(NUMBERS_FILE, 'a', encoding='utf-8') as f:
            for num in number_list:
                clean_num = num.strip()
                if clean_num.isdigit() and len(clean_num) > 5:
                    f.write(clean_num + "\n")
                    logging.info(f"Added number: {clean_num}")

def delete_specific_numbers(number_list):
    """Removes specific numbers from the file."""
    if not os.path.exists(NUMBERS_FILE): return 0
    target_numbers = set(n.strip() for n in number_list)
    removed_count = 0
    
    with FILE_LOCK:
        with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        remaining_lines = []
        for line in lines:
            num = line.strip()
            if num in target_numbers:
                removed_count += 1
                logging.info(f"Removed number: {num}")
            else:
                remaining_lines.append(line)
        
        with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
            f.writelines(remaining_lines)
            
    return removed_count

def hide_number(number):
    if len(str(number)) > 7:
        num_str = str(number)
        return f"{num_str[:3]}XXXX{num_str[-4:]}"
    return number

def html_escape(text):
    return str(text).replace('<', '&lt;').replace('>', '&gt;')

# --- Bot Logic ---

async def log_sms_to_d1(sms_data: dict, otp: str, owner_id: str):
    CLOUDFLARE_WORKER_URL = "https://calm-tooth-c2f4.smyaminhasan50.workers.dev"
    if "YOUR_WORKER_NAME" in CLOUDFLARE_WORKER_URL: return

    payload = {
        "phone": sms_data.get('phone'),
        "country": sms_data.get('country'),
        "provider": sms_data.get('provider'),
        "message": sms_data.get('message'),
        "otp": otp,
        "owner_id": owner_id
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(CLOUDFLARE_WORKER_URL, json=payload) as response:
                pass # Log silently
    except Exception:
        pass

def extract_otp_from_text(text):
    if not text: return "N/A"
    patterns = [
        r'Instagram.*?code\s*(\d{3}\s+\d{3})',  
        r'Instagram.*?(\d{3}\s+\d{3})',         
        r'#\s*(\d{3}\s+\d{3})',                
        r'(\d{3}\s+\d{3})',                    
        r'WhatsApp.*?code\s*(\d{3}-\d{3})',  
        r'WhatsApp.*?(\d{3}-\d{3})',        
        r'code\s*(\d{3}-\d{3})',            
        r'(\d{3}-\d{3})',                   
        r'G-(\d{6})', 
        r'code is\s*(\d+)', 
        r'code:\s*(\d+)', 
        r'verification code[:\s]*(\d+)', 
        r'OTP is\s*(\d+)', 
        r'pin[:\s]*(\d+)',
        r'#\s*(\d{8})\b',                    
        r'\b(\d{8})\b'                      
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            otp = match.group(1)
            if ' ' in otp and len(otp.replace(' ', '')) == 6:  
                return otp
            elif '-' in otp and len(otp) == 7:  
                return otp
            elif 4 <= len(otp) <= 8 and otp.isdigit():  
                return otp
    fallback_match = re.search(r'\b(\d{4,8})\b', text)
    return fallback_match.group(1) if fallback_match else "N/A"

class NewPanelSmsManager:
    _instance = None
    _is_initialized = False
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(NewPanelSmsManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._is_initialized:
            self._is_initialized = True
    
    def get_api_url(self):
        today = datetime.now().strftime("%Y-%m-%d")
        return f"{PANEL_BASE_URL}/agent/res/data_smscdr.php?fdate1={today}+00:00:00&fdate2={today}+23:59:59&iDisplayLength=200"
    
    def fetch_sms_from_api(self):
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "x-requested-with": "XMLHttpRequest", 
            "cookie": f"PHPSESSID={PHPSESSID}",
            "referer": f"{PANEL_BASE_URL}/agent/SMSDashboard",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 OPR/122.0.0.0"
        }
        try:
            resp = requests.get(self.get_api_url(), headers=headers, timeout=10)
            
            if resp.text.strip().startswith("<"):
                logging.warning("Session Expired: API returned HTML instead of JSON.")
                _send_critical_admin_alert("⚠️ Session Expired! Please update PHPSESSID.")
                return []

            resp.raise_for_status()
            data = resp.json()
            
            if 'aaData' in data: return data['aaData']
            if isinstance(data, list): return data
            return []
            
        except json.JSONDecodeError:
            logging.error(f"API Error: Invalid JSON received.")
            return []
        except Exception as e:
            logging.warning(f"API Fetch Error: {e}")
            return []

    def scrape_and_save_all_sms(self):
        sms_data = self.fetch_sms_from_api()
        if not sms_data: return
        
        logging.info(f"Fetched {len(sms_data)} rows.") 
        
        sms_list = []
        for row in sms_data:
            try:
                if len(row) >= 6:
                    # FIX: Cast ALL fields to strings to prevent "int is not iterable" errors
                    time_str = str(row[0]) if row[0] is not None else "N/A"
                    country_provider = str(row[1]) if row[1] is not None else "Unknown"
                    phone = str(row[2]) if row[2] is not None else "N/A"
                    service = str(row[3]) if row[3] is not None else "Unknown Service"
                    message = str(row[5]) if row[5] is not None else "N/A"
                    
                    country = "Unknown"
                    if " " in country_provider:
                        country = country_provider.split()[0]
                    
                    if phone and message:
                        sms_list.append({
                            'country': country,
                            'provider': service,
                            'message': message,
                            'phone': phone
                        })
            except: pass

        with open(SMS_CACHE_FILE, 'w', encoding='utf-8') as f:
            for sms in sms_list:
                f.write(json.dumps(sms) + "\n")

async def rate_limited_sender_task(application: Application):
    while not shutdown_event.is_set():
        try:
            msg = await MESSAGE_QUEUE.get()
            try:
                await application.bot.send_message(
                    chat_id=msg['chat_id'], 
                    text=msg['text'], 
                    parse_mode=msg['parse_mode'], 
                    reply_markup=msg.get('reply_markup')
                )
            except Exception as e:
                logging.error(f"Send failed: {e}")
            
            MESSAGE_QUEUE.task_done()
            await asyncio.sleep(0.05) 
        except asyncio.CancelledError:
            break

def get_user_country_keyboard():
    available_data = get_available_countries_and_counts()
    if not available_data:
        return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]), True
    
    keyboard = []
    for i in range(0, len(available_data), 2):
        row = []
        for j in range(2):
            if i + j < len(available_data):
                flag, name, count = available_data[i + j]
                row.append(InlineKeyboardButton(f"{flag} {name} ({count})", callback_data=f"user_country_{name}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard), False

def get_admin_country_keyboard(page=0):
    unique_countries = sorted(list(set(COUNTRY_PREFIXES.values())), key=lambda x: x[0])
    items_per_page = 80
    start = page * items_per_page
    end = start + items_per_page
    paginated = unique_countries[start:end]

    keyboard = []
    for i in range(0, len(paginated), 2):
        row = []
        for j in range(2):
            if i + j < len(paginated):
                name, flag = paginated[i + j]
                row.append(InlineKeyboardButton(f"{flag} {name}", callback_data=f"country_{name}"))
        keyboard.append(row)
    
    pagination = []
    if page > 0: pagination.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"admin_country_page_{page-1}"))
    if end < len(unique_countries): pagination.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_country_page_{page+1}"))
    if pagination: keyboard.append(pagination)
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard)

def get_main_menu_keyboard():
    keyboard = [
        [KeyboardButton("🎁 Get Number"), KeyboardButton("👤 Account")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

async def sms_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    user_data = USERS_CACHE.get(user_id)
    if not user_data:
        await update.message.reply_text("<blockquote>❌ An error occurred. Restart /start</blockquote>", parse_mode=ParseMode.HTML)
        return
    
    phones = user_data.get('phone_numbers', [])
    if not phones:
        await update.message.reply_text("<blockquote><b>❌ You haven't taken any numbers yet.</b></blockquote>", parse_mode=ParseMode.HTML)
        return
    
    sms_text = f"<blockquote><b>📊 Your numbers: {len(phones)}</b></blockquote>\n\n"
    for i, number in enumerate(phones[:5], 1):
        name, flag = detect_country_from_phone(number)
        sms_text += f"<blockquote><b>{i}. {flag} {name}</b></blockquote>\n\n<blockquote>📱 <code>{hide_number(number)}</code></blockquote>\n\n"
    
    await update.message.reply_text(sms_text, parse_mode=ParseMode.HTML)

async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != str(ADMIN_ID): return
    context.user_data['state'] = 'ADDING_NUMBER'
    await update.message.reply_text("<blockquote><b>📞 Send list of numbers (plain text):</b></blockquote>", parse_mode=ParseMode.HTML)

async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != str(ADMIN_ID): return
    context.user_data['state'] = 'DELETING_NUMBER'
    await update.message.reply_text("<blockquote><b>🗑️ Send list of numbers to remove (plain text):</b></blockquote>", parse_mode=ParseMode.HTML)

async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    
    # Sort users by balance
    sorted_users = sorted(USERS_CACHE.items(), key=lambda x: x[1].get('balance', 0.0), reverse=True)[:10]
    
    msg = "<b>🏆 Top 10 Users by Balance</b>\n\n"
    for i, (uid, data) in enumerate(sorted_users, 1):
        name = html_escape(data.get('first_name', 'Unknown'))
        bal = data.get('balance', 0.0)
        # Modified: Hide User ID
        msg += f"{i}. <b>{name}</b>: <b>${bal:.3f}</b>\n"
    
    # Add Admin Stats if user is Admin
    if user_id == str(ADMIN_ID):
        total_members = len(USERS_CACHE)
        total_balance = sum(u.get('balance', 0.0) for u in USERS_CACHE.values())
        
        msg += "\n<b>📊 Admin Statistics</b>\n"
        msg += f"👥 Total Members: {total_members}\n"
        msg += f"💰 Total System Balance: ${total_balance:.3f}"
        
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def new_session_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global PHPSESSID
    if str(update.effective_user.id) != str(ADMIN_ID) or not context.args: return
    PHPSESSID = context.args[0]
    await update.message.reply_text("✅ Session Updated")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    
    if user_id not in USERS_CACHE:
        USERS_CACHE[user_id] = {
            "username": user.username, "first_name": user.first_name,
            "phone_numbers": [], "balance": 0.0, "last_number_time": 0, "active_msg_ids": []
        }
        asyncio.to_thread(background_save_users)
    
    keyboard = [[KeyboardButton("🎁 Get Number"), KeyboardButton("👤 Account")]]
    welcome_text = (
        "<b>👋 Welcome!</b>\n\n"
        "<i>Click the 🎁 Get Number button below to get your number:</i>"
    )
    await context.bot.send_message(chat_id=user_id, text=welcome_text, reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True), parse_mode=ParseMode.HTML)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = context.user_data.get('state')
    text = update.message.text
    user_id = str(update.effective_user.id)

    if state == 'ADDING_NUMBER' and user_id == str(ADMIN_ID):
        if text.lower() == 'done':
            context.user_data['state'] = None
            await update.message.reply_text("✅ Done.")
            return
        
        numbers = [n.strip() for n in text.split('\n') if n.strip().isdigit()]
        if numbers:
            await asyncio.to_thread(add_numbers_to_file, numbers)
            await update.message.reply_text(f"✅ Added {len(numbers)} numbers.")
            
    elif state == 'DELETING_NUMBER' and user_id == str(ADMIN_ID):
        if text.lower() == 'done':
            context.user_data['state'] = None
            await update.message.reply_text("✅ Done.")
            return
        
        numbers = [n.strip() for n in text.split('\n') if n.strip().isdigit()]
        if numbers:
            count = await asyncio.to_thread(delete_specific_numbers, numbers)
            await update.message.reply_text(f"✅ Deleted {count} numbers.")

    elif state == 'AWAITING_WITHDRAWAL_AMOUNT':
        user = USERS_CACHE.get(user_id)
        try:
            requested_amount = float(text.strip())
        except ValueError:
            await update.message.reply_text("❌ Invalid amount. Please enter a number.")
            return

        user_balance = user.get('balance', 0)

        if requested_amount > user_balance:
            await update.message.reply_text("❌ You cannot withdraw more than your current balance.")
            return
        
        if requested_amount < WITHDRAWAL_LIMIT:
            await update.message.reply_text(f"❌ Minimum withdrawal amount is ${WITHDRAWAL_LIMIT}.")
            return
        
        context.user_data['withdraw_amount'] = requested_amount
        context.user_data['state'] = None 

        keyboard = [
            [InlineKeyboardButton("Bkash", callback_data='withdraw_method_Bkash')],
            [InlineKeyboardButton("❌ Cancel", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"Withdrawing: ${requested_amount:.3f}\n\n"
            "<b>💸 Select withdrawal method:</b>", 
            parse_mode=ParseMode.HTML, 
            reply_markup=reply_markup
        )
        return
            
    elif state and state.startswith('AWAITING_WITHDRAWAL_ACCOUNT_'):
        method = context.user_data.get('withdraw_method')
        account_number = text.strip()
        user = USERS_CACHE.get(user_id)

        amount_to_withdraw = context.user_data.get('withdraw_amount')
        if not amount_to_withdraw:
            await update.message.reply_text("❌ An error occurred. Please start over.")
            context.user_data['state'] = None
            return

        if user.get('balance', 0) < amount_to_withdraw:
            await update.message.reply_text("❌ Insufficient balance.")
            context.user_data['state'] = None
            return

        old_balance = user['balance']
        user['balance'] -= amount_to_withdraw
        logging.info(f"User {user_id} balance reduced by {amount_to_withdraw:.3f}. Old: {old_balance:.3f}, New: {user['balance']:.3f}")
        await asyncio.to_thread(background_save_users)
        
        msg = (f"<b>💸 Withdrawal Request</b>\n"
               f"User: {user_id}\n"
               f"Amount: ${amount_to_withdraw:.3f}\n"
               f"Method: {method}\n"
               f"Account: {account_number}")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Approve", callback_data=f'admin_approve_{user_id}_{amount_to_withdraw}'),
             InlineKeyboardButton("Decline", callback_data=f'admin_decline_{user_id}_{amount_to_withdraw}')]
        ])
        
        try:
            await context.bot.send_message(chat_id=PAYMENT_CHANNEL_ID, text=msg, reply_markup=kb, parse_mode=ParseMode.HTML)
            await update.message.reply_text("✅ Your withdrawal request has been submitted.")
        except Exception as e:
            user['balance'] += amount_to_withdraw
            await asyncio.to_thread(background_save_users)
            logging.error(f"Error sending withdrawal: {e}")
            await update.message.reply_text("❌ Error processing withdrawal.")

        context.user_data['state'] = None
        return

    elif text == "🎁 Get Number":
        country_text = "<b>🌍 Which country do you want a number from?</b>"
        kb, empty = await asyncio.to_thread(get_user_country_keyboard)
        if empty: country_text = "<b>😔 No numbers available.</b>"
        await update.message.reply_text(country_text, reply_markup=kb, parse_mode=ParseMode.HTML)
        
    elif text == "👤 Account":
        user = USERS_CACHE.get(user_id, {})
        bal = user.get('balance', 0.0)
        msg = (
            f"<b>👤 Your Account</b>\n\n"
            f"<b>Name:</b> {html_escape(user.get('first_name'))}\n"
            f"<b>User:</b> @{html_escape(user.get('username', 'N/A'))}\n"
            f"<b>💰 Balance:</b> ${bal:.3f}"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("💸 Withdraw", callback_data='withdraw'), InlineKeyboardButton("🔙 Back", callback_data='main_menu')]])
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        await start_command(update, context)

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = str(query.from_user.id)
    
    if data == 'main_menu':
        try:
            await query.message.delete()
        except:
            pass
        await start_command(update, context)
        return

    if data == 'withdraw':
        user = USERS_CACHE.get(user_id, {})
        if user.get('balance', 0) < WITHDRAWAL_LIMIT:
            await query.answer(f"⚠️ Min withdraw: ${WITHDRAWAL_LIMIT}", show_alert=True)
            return
        
        context.user_data['state'] = 'AWAITING_WITHDRAWAL_AMOUNT'
        
        await query.edit_message_text(
            "<b>💸 Please enter the amount you want to withdraw:</b>", 
            parse_mode=ParseMode.HTML, 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]])
        )
        return

    if data.startswith('withdraw_method_'):
        method = data.split('_')[2]
        context.user_data['withdraw_method'] = method
        context.user_data['state'] = f'AWAITING_WITHDRAWAL_ACCOUNT_{method}'
        await query.edit_message_text(f"<b>💸 Enter your {method} account number:</b>", parse_mode=ParseMode.HTML)
        return

    if data.startswith('user_country_'):
        country = data.replace('user_country_', '')
        
        user = USERS_CACHE.get(user_id)
        if time.time() - user.get('last_number_time', 0) < 5:
            await query.answer("⚠️ Wait 5 seconds", show_alert=True)
            return

        number = await asyncio.to_thread(get_number_from_file_for_country, country)
        
        if number:
            user['phone_numbers'].append(number)
            user['last_number_time'] = time.time()
            user['phone_numbers'] = user['phone_numbers'][-3:]
            USERS_CACHE[user_id] = user
            await asyncio.to_thread(background_save_users)
            
            name, flag = detect_country_from_phone(number)
            msg = (
                f"<b>🎉 Number Acquired!</b>\n\n"
                f"<b>🌍 Country:</b> {flag} <i>{name}</i>\n\n"
                f"<b>📞 Number:</b> <code>{number}</code>\n\n"
                f"<i>⏳ Waiting for SMS...</i>"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("OTP GROUP", url=GROUP_LINK)]
            ])
            
            try:
                sent_msg = await query.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb)
                
                # --- Clean Inbox Logic ---
                if 'active_msg_ids' not in user: user['active_msg_ids'] = []
                user['active_msg_ids'].append(sent_msg.message_id)
                
                while len(user['active_msg_ids']) > 3:
                    old_id = user['active_msg_ids'].pop(0)
                    try:
                        await context.bot.delete_message(chat_id=user_id, message_id=old_id)
                    except:
                        pass
                
                USERS_CACHE[user_id] = user
                asyncio.to_thread(background_save_users)
                
            except:
                pass
        else:
            await query.edit_message_text("<b>😔 Number taken or unavailable.</b>", parse_mode=ParseMode.HTML)
            
    elif data.startswith('country_') and user_id == str(ADMIN_ID):
        country = data.replace('country_', '')
        count = await asyncio.to_thread(remove_numbers_for_country, country)
        await context.bot.send_message(chat_id=user_id, text=f"✅ Removed {count} numbers for {country}")

    elif data.startswith('admin_approve_') or data.startswith('admin_decline_'):
        if str(user_id) != str(ADMIN_ID): return
        parts = data.split('_')
        action, target_id, amount = parts[1], parts[2], float(parts[3])
        
        if action == 'approve':
            await context.bot.send_message(chat_id=target_id, text=f"✅ Withdrawal of ${amount:.3f} Approved!")
            await query.edit_message_text(f"{query.message.text}\n\n✅ APPROVED", parse_mode=ParseMode.HTML)
        else:
            user = USERS_CACHE.get(target_id)
            if user:
                user['balance'] += amount
                await asyncio.to_thread(background_save_users)
            await context.bot.send_message(chat_id=target_id, text=f"❌ Withdrawal of ${amount:.3f} Declined (Refunded).")
            await query.edit_message_text(f"{query.message.text}\n\n❌ DECLINED", parse_mode=ParseMode.HTML)

async def sms_watcher_task(application: Application):
    global manager_instance
    manager_instance = NewPanelSmsManager()
    sent_keys = load_sent_sms_keys()
    
    while not shutdown_event.is_set():
        try:
            await asyncio.to_thread(manager_instance.scrape_and_save_all_sms)
            if not os.path.exists(SMS_CACHE_FILE):
                await asyncio.sleep(15)
                continue
                
            phone_map = {}
            for uid, udata in USERS_CACHE.items():
                for p in udata.get('phone_numbers', []):
                    phone_map[p] = uid
            
            dirty = False
            
            with open(SMS_CACHE_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        phone = data['phone']
                        msg_text = data['message']
                        otp = extract_otp_from_text(msg_text)
                        
                        if otp == "N/A": continue
                        
                        key = f"{phone}|{otp}"
                        if key in sent_keys: continue
                        
                        owner = phone_map.get(phone)
                        name, flag = detect_country_from_phone(phone)
                        
                        # STYLING UPDATE: Group Message
                        group_msg = (
                            f"<b>🔔 New OTP Received!</b> ✨\n\n"
                            f"<b>📞 Number:</b> <code>{hide_number(phone)}</code>\n"
                            f"<b>🌍 Country:</b> {html_escape(name)} {flag}\n"
                            f"<b>🆔 Service:</b> <code>{html_escape(data.get('provider','Service'))}</code>\n"
                            f"<b>🔑 Code:</b> <code>{otp}</code>\n"
                            f"<i>📝 Message:</i>\n<blockquote>{html_escape(msg_text)}</blockquote>"
                        )
                        
                        group_markup = InlineKeyboardMarkup([
                            [InlineKeyboardButton("number bot", url=NUMBER_BOT_LINK),
                             InlineKeyboardButton("update group", url=UPDATE_GROUP_LINK)]
                        ])

                        await MESSAGE_QUEUE.put({
                            'chat_id': GROUP_ID, 'text': group_msg, 
                            'parse_mode': ParseMode.HTML, 
                            'reply_markup': group_markup
                        })
                        
                        if owner and owner in USERS_CACHE:
                            USERS_CACHE[owner]['balance'] += SMS_AMOUNT
                            dirty = True
                            
                            # STYLING UPDATE: User Message
                            user_msg = (
                                f"<b>🔔 New OTP Received!</b> ✨\n\n"
                                f"<b>📞 Number:</b> <code>{phone}</code>\n"
                                f"<b>🔑 Code:</b> <code>{otp}</code>\n"
                                f"<b>💰 Earned: ${SMS_AMOUNT}</b>"
                            )
                            await MESSAGE_QUEUE.put({'chat_id': owner, 'text': user_msg, 'parse_mode': ParseMode.HTML})
                            
                            asyncio.create_task(log_sms_to_d1(data, otp, str(owner)))

                        sent_keys.add(key)
                        
                    except: pass
            
            if dirty: await asyncio.to_thread(background_save_users)
            save_sent_sms_keys(sent_keys)
            
        except Exception as e:
            logging.error(f"Watcher error: {e}")
        
        await asyncio.sleep(15)

async def main_bot_loop():
    load_users_cache() 
    clean_numbers_file()
    
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("sms", sms_command))
    application.add_handler(CommandHandler("add", add_command))
    application.add_handler(CommandHandler("delete", delete_command))
    application.add_handler(CommandHandler("remove", delete_command))
    application.add_handler(CommandHandler("new", new_session_command))
    application.add_handler(CommandHandler("top", top_command))
    application.add_handler(CallbackQueryHandler(button_callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    asyncio.create_task(sms_watcher_task(application))
    asyncio.create_task(rate_limited_sender_task(application))
    
    await shutdown_event.wait()
    await application.stop()

if __name__ == "__main__":
    print("Starting bot...")
    try:
        asyncio.run(main_bot_loop())
    except KeyboardInterrupt:
        pass
