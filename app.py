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
        logging.critical(f"{CONFIG_FILE} not found! Please create it before running the bot.")
        raise FileNotFoundError(f"{CONFIG_FILE} not found! Please create it.")
        
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
GROUP_LINK = "https://t.me/pgotp"
SMS_AMOUNT = 0.03  # $0.03 per OTP
WITHDRAWAL_LIMIT = 1.0  # Minimum $1.00 to withdraw

# New Panel Credentials
PANEL_BASE_URL = "http://51.89.99.105/NumberPanel"
PANEL_SMS_URL = f"{PANEL_BASE_URL}/agent/SMSCDRStats"
PHPSESSID = config.get('PHPSESSID', 'rpimjduka5o0bqp2hb3k1lrcp8')

# --- Global File Lock ---
# This is crucial to prevent database corruption when multiple users access the file
FILE_LOCK = threading.Lock()

# Available Countries
COUNTRIES = {
    "🇦🇨": "Ascension Island", "🇦🇩": "Andorra", "🇦🇪": "United Arab Emirates", "🇦🇫": "Afghanistan",
    "🇦🇬": "Antigua and Barbuda", "🇦🇮": "Anguilla", "🇦🇱": "Albania", "🇦🇲": "Armenia",
    "🇦🇴": "Angola", "🇦🇶": "Antarctica", "🇦🇷": "Argentina", "🇦🇸": "American Samoa",
    "🇦🇹": "Austria", "🇦🇺": "Australia", "🇦🇼": "Aruba", "🇦🇽": "Aland Islands",
    "🇦🇿": "Azerbaijan", "🇧🇦": "Bosnia and Herzegovina", "🇧🇧": "Barbados", "🇧🇩": "Bangladesh",
    "🇧🇪": "Belgium", "🇧🇫": "Burkina Faso", "🇧🇬": "Bulgaria", "🇧🇭": "Bahrain",
    "🇧🇮": "Burundi", "🇧🇯": "Benin", "🇧🇱": "Saint Barthelemy", "🇧🇲": "Bermuda",
    "🇧🇳": "Brunei", "🇧🇴": "Bolivia", "🇧🇶": "Caribbean Netherlands", "🇧🇷": "Brazil",
    "🇧🇸": "Bahamas", "🇧🇹": "Bhutan", "🇧🇻": "Bouvet Island", "🇧🇼": "Botswana",
    "🇧🇾": "Belarus", "🇧🇿": "Belize", "🇨🇦": "Canada", "🇨🇨": "Cocos (Keeling) Islands",
    "🇨🇩": "DR Congo", "🇨🇫": "Central African Republic", "🇨🇬": "Congo", "🇨🇭": "Switzerland",
    "🇨🇮": "Ivory Coast", "🇨🇰": "Cook Islands", "🇨🇱": "Chile", "🇨🇲": "Cameroon",
    "🇨🇳": "China", "🇨🇴": "Colombia", "🇨🇵": "Clipperton Island", "🇨🇷": "Costa Rica",
    "🇨🇺": "Cuba", "🇨🇻": "Cape Verde", "🇨🇼": "Curaçao", "🇨🇽": "Christmas Island",
    "🇨🇾": "Cyprus", "🇨🇿": "Czech Republic", "🇩🇪": "Germany", "🇩🇬": "Diego Garcia",
    "🇩🇯": "Djibouti", "🇩🇰": "Denmark", "🇩🇲": "Dominica", "🇩🇴": "Dominican Republic",
    "🇩🇿": "Algeria", "🇪🇦": "Ceuta & Melilla", "🇪🇨": "Ecuador", "🇪🇪": "Estonia",
    "🇪🇬": "Egypt", "🇪🇭": "Western Sahara", "🇪🇷": "Eritrea", "🇪🇸": "Spain",
    "🇪🇹": "Ethiopia", "🇪🇺": "European Union", "🇫🇮": "Finland", "🇫🇯": "Fiji",
    "🇫🇰": "Falkland Islands (Malvinas)", "🇫🇲": "Micronesia", "🇫🇴": "Faroe Islands", "🇫🇷": "France",
    "🇬🇦": "Gabon", "🇬🇧": "United Kingdom", "🇬🇩": "Grenada", "🇬🇪": "Georgia",
    "🇬🇫": "French Guiana", "🇬🇬": "Guernsey", "🇬🇭": "Ghana", "🇬🇮": "Gibraltar",
    "🇬🇱": "Greenland", "🇬🇲": "Gambia", "🇬🇳": "Guinea", "🇬🇵": "Guadeloupe",
    "🇬🇶": "Equatorial Guinea", "🇬🇷": "Greece", "🇬🇸": "South Georgia and the South Sandwich Islands", "🇬🇹": "Guatemala",
    "🇬🇺": "Guam", "🇬🇼": "Guinea-Bissau", "🇬🇾": "Guyana", "🇭🇰": "Hong Kong",
    "🇭🇲": "Heard Island and McDonald Islands", "🇭🇳": "Honduras", "🇭🇷": "Croatia", "🇭🇹": "Haiti",
    "🇭🇺": "Hungary", "🇮🇨": "Canary Islands", "🇮🇩": "Indonesia", "🇮🇪": "Ireland",
    "🇮🇱": "Israel", "🇮🇲": "Isle of Man", "🇮🇳": "India", "🇮🇴": "British Indian Ocean Territory",
    "🇮🇶": "Iraq", "🇮🇷": "Iran", "🇮🇸": "Iceland", "🇮🇹": "Italy",
    "🇯🇪": "Jersey", "🇯🇲": "Jamaica", "🇯🇴": "Jordan", "🇯🇵": "Japan",
    "🇰🇪": "Kenya", "🇰🇬": "Kyrgyzstan", "🇰🇭": "Cambodia", "🇰🇮": "Kiribati",
    "🇰🇲": "Comoros", "🇰🇳": "Saint Kitts and Nevis", "🇰🇵": "North Korea", "🇰🇷": "South Korea",
    "🇰🇼": "Kuwait", "🇰🇾": "Cayman Islands", "🇰🇿": "Kazakhstan", "🇱🇦": "Laos",
    "🇱🇧": "Lebanon", "🇱🇨": "Saint Lucia", "🇱🇮": "Liechtenstein", "🇱🇰": "Sri Lanka",
    "🇱🇷": "Liberia", "🇱🇸": "Lesotho", "🇱🇹": "Lithuania", "🇱🇺": "Luxembourg",
    "🇱🇻": "Latvia", "🇱🇾": "Libya", "🇲🇦": "Morocco", "🇲🇨": "Monaco",
    "🇲🇩": "Moldova", "🇲🇪": "Montenegro", "🇲🇫": "Saint Martin", "🇲🇬": "Madagascar",
    "🇲🇭": "Marshall Islands", "🇲🇰": "North Macedonia", "🇲🇱": "Mali", "🇲🇲": "Myanmar",
    "🇲🇳": "Mongolia", "🇲🇴": "Macao", "🇲🇵": "Northern Mariana Islands", "🇲🇶": "Martinique",
    "🇲🇷": "Mauritania", "🇲🇸": "Montserrat", "🇲🇹": "Malta", "🇲🇺": "Mauritius",
    "🇲🇻": "Maldives", "🇲🇼": "Malawi", "🇲🇽": "Mexico", "🇲🇾": "Malaysia",
    "🇲🇿": "Mozambique", "🇳🇦": "Namibia", "🇳🇨": "New Caledonia", "🇳🇪": "Niger",
    "🇳🇫": "Norfolk Island", "🇳🇬": "Nigeria", "🇳🇮": "Nicaragua", "🇳🇱": "Netherlands",
    "🇳🇴": "Norway", "🇳🇵": "Nepal", "🇳🇷": "Nauru", "🇳🇺": "Niue",
    "🇳🇿": "New Zealand", "🇴🇲": "Oman", "🇵🇦": "Panama", "🇵🇪": "Peru",
    "🇵🇫": "French Polynesia", "🇵🇬": "Papua New Guinea", "🇵🇭": "Philippines", "🇵🇰": "Pakistan",
    "🇵🇱": "Poland", "🇵🇲": "Saint Pierre and Miquelon", "🇵🇳": "Pitcairn Islands", "🇵🇷": "Puerto Rico",
    "🇵🇸": "Palestine", "🇵🇹": "Portugal", "🇵🇼": "Palau", "🇵🇾": "Paraguay",
    "🇶🇦": "Qatar", "🇷🇪": "Reunion", "🇷🇴": "Romania", "🇷🇸": "Serbia",
    "🇷🇺": "Russia", "🇷🇼": "Rwanda", "🇸🇦": "Saudi Arabia", "🇸🇧": "Solomon Islands",
    "🇸🇨": "Seychelles", "🇸🇩": "Sudan", "🇸🇪": "Sweden", "🇸🇬": "Singapore",
    "🇸🇭": "St. Helena", "🇸🇮": "Slovenia", "🇸🇯": "Svalbard and Jan Mayen", "🇸🇰": "Slovakia",
    "🇸🇱": "Sierra Leone", "🇸🇲": "San Marino", "🇸🇳": "Senegal", "🇸🇴": "Somalia",
    "🇸🇷": "Suriname", "🇸🇸": "South Sudan", "🇸🇹": "Sao Tome and Principe", "🇸🇻": "El Salvador",
    "🇸🇽": "Sint Maarten", "🇸🇾": "Syria", "🇸🇿": "Eswatini", "🇹🇦": "Tristan da Cunha",
    "🇹🇨": "Turks and Caicos Islands", "🇹🇩": "Chad", "🇹🇫": "French Southern Territories", "🇹🇬": "Togo",
    "🇹🇭": "Thailand", "🇹🇯": "Tajikistan", "🇹🇰": "Tokelau", "🇹🇱": "Timor-Leste",
    "🇹🇲": "Turkmenistan", "🇹🇳": "Tunisia", "🇹🇴": "Tonga", "🇹🇷": "Turkey",
    "🇹🇹": "Trinidad & Tobago", "🇹🇻": "Tuvalu", "🇹🇼": "Taiwan", "🇹🇿": "Tanzania",
    "🇺🇦": "Ukraine", "🇺🇬": "Uganda", "🇺🇲": "United States Outlying Islands", "🇺🇳": "United Nations",
    "🇺🇸": "United States", "🇺🇾": "Uruguay", "🇺🇿": "Uzbekistan", "🇻🇦": "Vatican City",
    "🇻🇨": "Saint Vincent and the Grenadines", "🇻🇪": "Venezuela", "🇻🇬": "British Virgin Islands", "🇻🇮": "United States Virgin Islands",
    "🇻🇳": "Vietnam", "🇻🇺": "Vanuatu", "🇼🇫": "Wallis and Futuna", "🇼🇸": "Samoa",
    "🇽🇰": "Kosovo", "🇾🇪": "Yemen", "🇾🇹": "Mayotte", "🇿🇦": "South Africa",
    "🇿🇲": "Zambia", "🇿🇼": "Zimbabwe", "🏴󠁧󠁢󠁥󠁮󠁧󠁿": "England", "🏴󠁧󠁢󠁳󠁣󠁴󠁿": "Scotland",
    "🏴󠁧󠁢󠁷󠁬󠁳󠁿": "Wales"
}

# Available Social Media Platforms
SOCIAL_PLATFORMS = [
    "WhatsApp", "AUTHENTIFY", "Facebook", "Verify", "InfobankCrp", "OKTA",
    "InfoSMS", "NHNcorp", "Apple", "NOTICE", "Binance", "Sony", "PGUVERCINI",
    "Winbit", "FREEDOM", "Google", "Steam", "AIRBNB", "Stockmann", "IQOS",
    "TCELLWIFI", "EpicGames", "Bybit", "TK INFO", "Booking.com", "Kapitalbank",
    "DiDi", "PMSM_Ltd", "Huawei", "PEGASUS", "Moneybo", "BOG.GE", "1win",
    "Microsoft", "Instagram", "Telegram", "Snapchat", "TikTok", "Twitter (X)",
    "LinkedIn", "Pinterest", "Reddit", "Discord", "Threads", "WeChat",
    "Viber", "Skype", "Line", "Signal", "Clubhouse", "Tumblr", "Messenger",
    "Quora", "KakaoTalk", "Imo"
]

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

# Setup logging
logging.basicConfig(filename='bot_error.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')

# Disable HTTP request logging
logging.getLogger('telegram').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)
logging.getLogger('httpcore').setLevel(logging.ERROR)

# Bangladesh Standard Time (BST) is UTC+6
BST_OFFSET = timedelta(hours=6)
BST_TIMEZONE = timezone(BST_OFFSET)

def get_bst_now():
    """Get current time in Bangladesh Standard Time."""
    return datetime.now(BST_TIMEZONE)

# --- Helper Functions ---

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

def load_sent_sms_keys():
    return set(load_json_data(SENT_SMS_FILE, []))

def save_sent_sms_keys(keys):
    save_json_data(SENT_SMS_FILE, list(keys))

def _send_critical_admin_alert(message):
    """Sends a critical notification to the admin immediately using a sync Bot instance."""
    global LAST_SESSION_FAILURE_NOTIFICATION
    if time.time() - LAST_SESSION_FAILURE_NOTIFICATION < 600:
        return
        
    try:
        sync_bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
        sync_bot.send_message(
            chat_id=ADMIN_ID, 
            text=f"<b>{message}</b>", 
            parse_mode=ParseMode.HTML
        )
        LAST_SESSION_FAILURE_NOTIFICATION = time.time()
    except Exception as e:
        logging.error(f"Failed to send critical admin notification: {e}")

async def log_sms_to_d1(sms_data: dict, otp: str, owner_id: str):
    """
    Asynchronously sends SMS data to a Cloudflare Worker which logs it to D1.
    """
    CLOUDFLARE_WORKER_URL = "https://calm-tooth-c2f4.smyaminhasan50.workers.dev"
    
    if CLOUDFLARE_WORKER_URL == "https://YOUR_WORKER_NAME.YOUR_ACCOUNT.workers.dev":
        logging.warning("Cloudflare Worker URL is not set. Skipping D1 log.")
        return

    payload = {
        "phone": sms_data.get('phone'),
        "country": sms_data.get('country'),
        "provider": sms_data.get('provider'),
        "message": sms_data.get('message'),
        "otp": otp,
        "owner_id": owner_id
    }
    
    headers = {
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(CLOUDFLARE_WORKER_URL, json=payload, headers=headers) as response:
                if response.status == 201:
                    logging.info(f"Successfully logged SMS for {payload['phone']} to D1.")
                else:
                    logging.error(f"Failed to log SMS to D1. Status: {response.status}, Body: {await response.text()}")
    except Exception as e:
        logging.error(f"Error connecting to Cloudflare Worker: {e}")

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

def get_number_from_file_for_platform(country, platform):
    if not os.path.exists(NUMBERS_FILE):
        return None
    
    with FILE_LOCK:
        with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        if not lines:
            return None
        
        for i, line in enumerate(lines):
            try:
                number_info = json.loads(line)
                if (number_info.get("country") == country and 
                    number_info.get("platform") == platform):
                    number = number_info.get("number")
                    if number:
                        lines.pop(i)
                        with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
                            for remaining_line in lines:
                                f.write(remaining_line + "\n")
                        return number
            except:
                if country == "Kenya":
                    number = line
                    if number:
                        lines.pop(i)
                        with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
                            for remaining_line in lines:
                                f.write(remaining_line + "\n")
                        return number
    
    return None

def add_number_to_file(number, country=None, platform=None):
    with FILE_LOCK:
        number_info = {
            "number": number,
            "country": country,
            "platform": platform,
            "added_date": get_bst_now().isoformat()
        }
        
        logging.error(f"Adding number to file: {number_info}")
        
        existing_content = []
        if os.path.exists(NUMBERS_FILE):
            try:
                with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
                    existing_content = f.readlines()
            except Exception as e:
                logging.error(f"Error reading existing file: {e}")
                existing_content = []
        
        try:
            with open(NUMBERS_FILE, 'a', encoding='utf-8') as f:
                f.write(json.dumps(number_info, ensure_ascii=False) + "\n")
            logging.error(f"Successfully added number {number} to file")
        except Exception as e:
            logging.error(f"Error writing to file: {e}")
            try:
                with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
                    f.writelines(existing_content)
            except Exception as restore_error:
                logging.error(f"Failed to restore file: {restore_error}")

def remove_numbers_for_platforms(country, platforms):
    with FILE_LOCK:
        if not os.path.exists(NUMBERS_FILE):
            return 0
        
        try:
            with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception as e:
            logging.error(f"Error reading file for removal: {e}")
            return 0
        
        remaining_lines = []
        removed_count = 0
        
        for line in lines:
            line = line.strip()
            if not line:
                remaining_lines.append(line + "\n")
                continue
            
            try:
                number_info = json.loads(line)
                if (number_info.get("country") == country and 
                    number_info.get("platform") in platforms):
                    removed_count += 1
                    logging.error(f"Removed number: {number_info}")
                else:
                    remaining_lines.append(line + "\n")
            except:
                if country == "Kenya":
                    removed_count += 1
                    logging.error(f"Removed old format number: {line}")
                else:
                    remaining_lines.append(line + "\n")
        
        try:
            with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
                f.writelines(remaining_lines)
            logging.error(f"Successfully removed {removed_count} numbers from file")
        except Exception as e:
            logging.error(f"Error writing file after removal: {e}")
            return 0
        
        return removed_count

def get_available_countries_for_platform(platform):
    if not os.path.exists(NUMBERS_FILE):
        return []
    
    countries = set()
    with FILE_LOCK:
        with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    number_info = json.loads(line)
                    if number_info.get("platform") == platform:
                        country = number_info.get("country")
                        if country:
                            countries.add(country)
                except:
                    if platform in ["WhatsApp", "Facebook", "Instagram"]:
                        countries.add("Kenya")
    
    return list(countries)

def get_number_count_for_country_and_platform(country, platform):
    if not os.path.exists(NUMBERS_FILE):
        return 0
    
    count = 0
    with FILE_LOCK:
        with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    number_info = json.loads(line)
                    if (number_info.get("country") == country and 
                        number_info.get("platform") == platform):
                        count += 1
                except:
                    if country == "Kenya" and platform in ["WhatsApp", "Facebook", "Instagram"]:
                        count += 1
    
    return count

def get_admin_country_keyboard(page=0):
    keyboard = []
    countries_list = list(COUNTRIES.items())
    items_per_page = 80

    start_index = page * items_per_page
    end_index = start_index + items_per_page
    
    paginated_countries = countries_list[start_index:end_index]

    for i in range(0, len(paginated_countries), 2):
        row = []
        for j in range(2):
            if i + j < len(paginated_countries):
                flag, country = paginated_countries[i + j]
                row.append(InlineKeyboardButton(f"{flag} {country}", callback_data=f"country_{flag}"))
        keyboard.append(row)
    
    pagination_row = []
    if page > 0:
        pagination_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"admin_country_page_{page-1}"))
    
    if end_index < len(countries_list):
        pagination_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_country_page_{page+1}"))
    
    if pagination_row:
        keyboard.append(pagination_row)
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard)

def get_admin_social_keyboard(selected_platforms=None):
    if selected_platforms is None:
        selected_platforms = set()
    
    keyboard = []
    for i in range(0, len(SOCIAL_PLATFORMS), 2):
        row = []
        for j in range(2):
            if i + j < len(SOCIAL_PLATFORMS):
                platform = SOCIAL_PLATFORMS[i + j]
                button_text = f"✅ {platform}" if platform in selected_platforms else platform
                row.append(InlineKeyboardButton(button_text, callback_data=f"social_{platform}"))
        keyboard.append(row)
    
    if selected_platforms:
        keyboard.append([InlineKeyboardButton("✅ Done - Continue", callback_data="social_done")])
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard)

def get_main_menu_keyboard():
    # Use ReplyKeyboardMarkup for persistent buttons
    keyboard = [
        [KeyboardButton("🎁 Get Number"), KeyboardButton("👤 Account")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_user_social_keyboard():
    keyboard = []
    available_platforms = []
    for platform in SOCIAL_PLATFORMS:
        available_countries = get_available_countries_for_platform(platform)
        if available_countries:
            available_platforms.append(platform)
    
    if not available_platforms:
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]
        return InlineKeyboardMarkup(keyboard)
    
    for i in range(0, len(available_platforms), 2):
        row = []
        for j in range(2):
            if i + j < len(available_platforms):
                platform = available_platforms[i + j]
                row.append(InlineKeyboardButton(platform, callback_data=f"user_social_{platform}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard)

def get_country_keyboard_for_platform(platform):
    available_countries = get_available_countries_for_platform(platform)
    
    if not available_countries:
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]
        return InlineKeyboardMarkup(keyboard), True
    
    keyboard = []
    countries_list = []
    
    for flag, country in COUNTRIES.items():
        if country in available_countries:
            count = get_number_count_for_country_and_platform(country, platform)
            if count > 0:
                countries_list.append((flag, country, count))
    
    for i in range(0, len(countries_list), 2):
        row = []
        for j in range(2):
            if i + j < len(countries_list):
                flag, country, count = countries_list[i + j]
                row.append(InlineKeyboardButton(f"{flag} {country} ({count})", callback_data=f"user_country_{flag}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard), False

def get_number_info(phone_number):
    if not os.path.exists(NUMBERS_FILE):
        return None, None, None
    
    with FILE_LOCK:
        with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    number_info = json.loads(line)
                    if number_info.get("number") == phone_number:
                        country = number_info.get("country")
                        platform = number_info.get("platform")
                        flag = None
                        if country:
                            for f, c in COUNTRIES.items():
                                if c == country:
                                    flag = f
                                    break
                        return country, platform, flag
                except:
                    if line == phone_number:
                        return "Kenya", "WhatsApp", "🇰🇪"
    
    return None, None, None

def html_escape(text):
    return str(text).replace('<', '&lt;').replace('>', '&gt;')

def hide_number(number):
    if len(str(number)) > 7:
        num_str = str(number)
        return f"{num_str[:3]}XXXX{num_str[-4:]}"
    return number

def detect_country_from_phone(phone):
    if not phone:
        return "Unknown", "🌍"
    
    phone_str = str(phone).replace("+", "").replace(" ", "").replace("-", "")
    
    country_codes = {
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
    
    for length in [3, 2, 1]:
        if len(phone_str) >= length:
            prefix = phone_str[:length]
            if prefix in country_codes:
                return country_codes[prefix]
    
    return "Unknown", "🌍"

class NewPanelSmsManager:
    _instance = None
    _is_initialized = False
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(NewPanelSmsManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._is_initialized:
            self._initialize_api()
    
    def _initialize_api(self):
        self._is_initialized = True
        logging.info("API-based SMS manager initialized")
    
    def get_api_url(self):
        today = datetime.now().strftime("%Y-%m-%d")
        return f"{PANEL_BASE_URL}/agent/res/data_smscdr.php?fdate1={today}+00:00:00&fdate2={today}+23:59:59&iDisplayLength=200"
    
    def fetch_sms_from_api(self):
        session_check_headers = {
            "cookie": f"PHPSESSID={PHPSESSID}",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 OPR/122.0.0.0"
        }
        try:
            html_resp = requests.get(PANEL_SMS_URL, headers=session_check_headers, timeout=10)
            html_resp.raise_for_status()
            soup = BeautifulSoup(html_resp.text, "html.parser")
            title_tag = soup.find('title')
            
            if title_tag and 'Login' in title_tag.get_text():
                logging.error("Session check: appears to be login page. Update PHPSESSID.")
                error_msg = f"🚨 CRITICAL: Panel Session Expired! Update PHPSESSID in config.txt IMMEDIATELY. Time: {get_bst_now().strftime('%H:%M:%S')} BST"
                _send_critical_admin_alert(error_msg)
                return []
        except Exception as e:
            logging.warning(f"Initial session check failed: {e}")
            return [] 

        data_url = self.get_api_url()
        data_headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "x-requested-with": "XMLHttpRequest", 
            "cookie": f"PHPSESSID={PHPSESSID}",
            "referer": f"{PANEL_BASE_URL}/agent/SMSDashboard",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 OPR/122.0.0.0"
        }
        
        retries = 3
        for attempt in range(retries):
            try:
                data_resp = requests.get(data_url, headers=data_headers, timeout=10)
                data_resp.raise_for_status()
                json_data = data_resp.json()
                
                if 'aaData' in json_data and isinstance(json_data['aaData'], list):
                    return json_data['aaData']
                elif isinstance(json_data, list):
                    return json_data 

                logging.warning(f"Data fetch attempt {attempt + 1}/{retries}: JSON missing 'aaData' or unexpected format.")
                
            except json.JSONDecodeError:
                logging.error(f"Data fetch attempt {attempt + 1}/{retries}: Response is not valid JSON.")
            except Exception as data_err:
                logging.warning(f"Data fetch attempt {attempt + 1}/{retries} failed: {data_err}")
                
            if attempt < retries - 1:
                time.sleep(5)
        
        logging.error("SMS data fetch failed after all attempts.")
        return []

    def scrape_and_save_all_sms(self):
        try:
            sms_data = self.fetch_sms_from_api()
            logging.info(f"Fetched {len(sms_data)} rows from API.") 
            sms_list = []
            
            for row in sms_data:
                try:
                    if len(row) >= 6:
                        time_str = row[0] if len(row) > 0 else "N/A"
                        country_provider = row[1] if len(row) > 1 else "Unknown"
                        phone = row[2] if len(row) > 2 else "N/A"
                        service = row[3] if len(row) > 3 else "Unknown Service"
                        message = row[5] if len(row) > 5 else "N/A"
                        
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
                except Exception as e:
                    logging.warning(f"Could not parse SMS row: {e}")

            logging.info(f"Processed {len(sms_list)} valid SMS entries.") 
            with FILE_LOCK:
                with open(SMS_CACHE_FILE, 'w', encoding='utf-8') as f:
                    for sms in sms_list:
                        f.write(json.dumps(sms) + "\n")
            
        except Exception as e:
            logging.error(f"SMS API fetch failed: {e}")

    def cleanup(self):
        pass

async def rate_limited_sender_task(application: Application):
    while not shutdown_event.is_set():
        try:
            message_data = await MESSAGE_QUEUE.get()
            
            chat_id = message_data['chat_id']
            text = message_data['text']
            parse_mode = message_data.get('parse_mode', ParseMode.HTML)
            reply_markup = message_data.get('reply_markup')
            
            retry_attempts = 5
            for attempt in range(retry_attempts):
                try:
                    await application.bot.send_message(
                        chat_id=chat_id, 
                        text=text, 
                        parse_mode=parse_mode, 
                        reply_markup=reply_markup
                    )
                    break 
                except error.RetryAfter as e:
                    sleep_time = e.retry_after + 1
                    logging.warning(f"Telegram rate limit hit. Sleeping for {sleep_time}s. Chat ID: {chat_id}")
                    await asyncio.sleep(sleep_time)
                except Exception as e:
                    logging.error(f"Failed to send message to {chat_id}: {e}")
                    if attempt == retry_attempts - 1:
                        logging.error(f"Giving up on message to {chat_id}.")
                    else:
                        await asyncio.sleep(2)
            
            MESSAGE_QUEUE.task_done()
            await asyncio.sleep(0.05) 
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"Error in rate_limited_sender_task: {e}")
            await asyncio.sleep(1) 

def get_main_menu_keyboard():
    keyboard = [
        [KeyboardButton("🎁 Get Number"), KeyboardButton("👤 Account")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

async def sms_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id)
    
    if not user_data:
        await update.message.reply_text("<blockquote>❌ An error occurred.</blockquote>\n\n<blockquote>Please restart with /start command.</blockquote>", parse_mode=ParseMode.HTML)
        return
    
    phone_numbers = user_data.get('phone_numbers', [])
    if not phone_numbers:
        await update.message.reply_text(
            "<blockquote><b>📱 SMS Information</b></blockquote>\n\n"
            "<blockquote><b>❌ You haven't taken any numbers yet.</b></blockquote>\n\n"
            "<blockquote><b>To get numbers:</b></blockquote>\n\n"
            "<blockquote>🎁 Click Get Number button</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return
    
    sms_text = "<blockquote><b>📱 SMS Information</b></blockquote>\n\n"
    sms_text += f"<blockquote><b>📊 Your numbers: {len(phone_numbers)}</b></blockquote>\n\n"
    
    for i, number in enumerate(phone_numbers[:5], 1):
        number_country, number_platform, number_flag = get_number_info(number)
        display_country = number_country if number_country else "Unknown"
        display_platform = number_platform if number_platform else "Unknown"
        display_flag = number_flag if number_flag else "🌍"
        
        sms_text += f"<blockquote><b>{i}. {display_flag} {display_country}</b></blockquote>\n\n<blockquote>📱 <code>{hide_number(number)}</code></blockquote>\n\n<blockquote>🔗 Platform: {display_platform}</blockquote>\n\n"
    
    if len(phone_numbers) > 5:
        sms_text += f"<blockquote><b>... and {len(phone_numbers) - 5} more numbers</b></blockquote>\n\n"
    
    sms_text += "<blockquote><b>💡 Tips:</b></blockquote>\n\n"
    sms_text += "<blockquote><blockquote>• SMS will be sent to you automatically</blockquote>\n\n<blockquote>• Click Get Number button to get new numbers</blockquote></blockquote>"
    
    await update.message.reply_text(sms_text, parse_mode=ParseMode.HTML)

async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id != str(ADMIN_ID):
        await update.message.reply_text("<blockquote><b>❌ This command can only be used by admin.</b></blockquote>", parse_mode=ParseMode.HTML)
        return
    
    country_text = "<blockquote><b>🌍 Which country's numbers do you want to add? (Page 1)</b></blockquote>"
    await update.message.reply_text(
        country_text,
        reply_markup=get_admin_country_keyboard(page=0),
        parse_mode=ParseMode.HTML
    )

async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id != str(ADMIN_ID):
        await update.message.reply_text("<blockquote><b>❌ This command can only be used by admin.</b></blockquote>", parse_mode=ParseMode.HTML)
        return
    
    country_text = "<blockquote><b>🗑️ Which country's numbers do you want to remove? (Page 1)</b></blockquote>"
    context.user_data['state'] = 'REMOVING_NUMBER'
    await update.message.reply_text(
        country_text,
        reply_markup=get_admin_country_keyboard(page=0),
        parse_mode=ParseMode.HTML
    )

async def new_session_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global PHPSESSID
    user_id = str(update.effective_user.id)
    if user_id != str(ADMIN_ID):
        await update.message.reply_text("<blockquote><b>❌ This command can only be used by admin.</b></blockquote>", parse_mode=ParseMode.HTML)
        return

    if not context.args:
        await update.message.reply_text(
            "<blockquote><b>Usage:</b> /new &lt;NEW_PHPSESSID&gt;</blockquote>\n\n"
            "<blockquote><b>Example:</b> /new abc123def456...</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return
        
    new_session_id = context.args[0]
    
    try:
        config_parser = configparser.ConfigParser()
        config_parser.read(CONFIG_FILE, encoding='utf-8')
        if 'Settings' not in config_parser:
            config_parser['Settings'] = {}
        config_parser['Settings']['PHPSESSID'] = new_session_id
        with open(CONFIG_FILE, 'w', encoding='utf-8') as configfile:
            config_parser.write(configfile)
        
        PHPSESSID = new_session_id
        await update.message.reply_text(
            f"<blockquote><b>✅ PHPSESSID updated successfully!</b></blockquote>\n\n"
            f"<blockquote><b>New ID:</b> <code>{html_escape(new_session_id)}</code></blockquote>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logging.error(f"Failed to update PHPSESSID: {e}")
        await update.message.reply_text(
            f"<blockquote><b>❌ Failed to update PHPSESSID. Check logs.</b></blockquote>\n\n"
            f"<blockquote>Error: {e}</blockquote>",
            parse_mode=ParseMode.HTML
        )

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    users_data = load_json_data(USERS_FILE, {})

    if user_id not in users_data:
        users_data[user_id] = {
            "username": user.username, 
            "first_name": user.first_name, 
            "phone_numbers": [],
            "balance": 0.0, 
            "last_number_time": 0
        }
    else:
        if "balance" not in users_data[user_id]:
            users_data[user_id]["balance"] = 0.0
    
    save_json_data(USERS_FILE, users_data)
    welcome_text = (
        "<blockquote><b>👋 Welcome!</b></blockquote>\n\n"
        "<blockquote>Click the 🎁 Get Number button below to get your number:</blockquote>\n\n"
    )

    await update.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode=ParseMode.HTML)

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except error.BadRequest as e:
        logging.warning(f"Could not answer callback query: {e}")

    user_id = str(query.from_user.id)
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id)
    back_button = [InlineKeyboardButton("🔙 Back", callback_data='main_menu')]

    if not user_data:
        try:
            await query.edit_message_text("<blockquote>❌ An error occurred.</blockquote>\n\n<blockquote>Please restart with /start command.</blockquote>", parse_mode=ParseMode.HTML)
        except error.BadRequest:
            pass
        return
        
    if query.data == 'main_menu':
        try:
            await query.message.delete()
        except Exception:
            pass
        await start_command(update, context)
        return

    if query.data == 'withdraw':
        balance = user_data.get('balance', 0.0)
        if balance < WITHDRAWAL_LIMIT:
            await query.answer(f"⚠️ Minimum withdrawal is ${WITHDRAWAL_LIMIT}", show_alert=True)
            return
        
        context.user_data['state'] = 'AWAITING_WITHDRAWAL_INFO'
        withdraw_text = (
            f"<blockquote><b>💸 Withdrawal Request</b></blockquote>\n\n"
            f"<blockquote><b>Balance:</b> ${balance:.2f}</blockquote>\n\n"
            f"<blockquote><b>Minimum:</b> ${WITHDRAWAL_LIMIT}</blockquote>\n\n"
            "<blockquote><b>Please send your payment details (e.g., Wallet Address, ID) below:</b></blockquote>"
        )
        await query.edit_message_text(withdraw_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([back_button]))
        return

    if query.data.startswith('admin_approve_') or query.data.startswith('admin_decline_'):
        if str(user_id) != str(ADMIN_ID):
            await query.answer("❌ Admin only!", show_alert=True)
            return
            
        parts = query.data.split('_')
        action = f"{parts[0]}_{parts[1]}"
        target_uid = parts[2]
        amount = float(parts[3])
        
        if action == 'admin_approve':
            new_text = query.message.text + "\n\n✅ <b>APPROVED</b>"
            try:
                await context.bot.send_message(
                    chat_id=target_uid,
                    text=f"<blockquote><b>✅ Your withdrawal of ${amount} has been approved!</b></blockquote>",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logging.error(f"Failed to notify user {target_uid} of approval: {e}")
        else:
            target_data = users_data.get(target_uid)
            if target_data:
                target_data['balance'] = target_data.get('balance', 0.0) + amount
                users_data[target_uid] = target_data
                save_json_data(USERS_FILE, users_data)
                
            new_text = query.message.text + "\n\n❌ <b>DECLINED (Refunded)</b>"
            try:
                await context.bot.send_message(
                    chat_id=target_uid,
                    text=f"<blockquote><b>❌ Your withdrawal of ${amount} has been declined and refunded.</b></blockquote>",
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logging.error(f"Failed to notify user {target_uid} of decline: {e}")

        await query.edit_message_text(text=new_text, parse_mode=ParseMode.HTML, reply_markup=None)
        return

    if query.data.startswith('admin_country_page_'):
        page = int(query.data.split('_')[-1])
        state = context.user_data.get('state')
        if state == 'REMOVING_NUMBER':
            text = f"<blockquote><b>🗑️ Which country's numbers do you want to remove? (Page {page + 1})</b></blockquote>"
        else:
            text = f"<blockquote><b>🌍 Which country's numbers do you want to add? (Page {page + 1})</b></blockquote>"

        try:
            await query.edit_message_text(
                text=text,
                reply_markup=get_admin_country_keyboard(page=page),
                parse_mode=ParseMode.HTML
            )
        except error.BadRequest:
            pass
        return

    elif query.data.startswith('user_social_'):
        platform = query.data.split('user_social_')[1]
        context.user_data['selected_platform'] = platform
        country_text = f"<blockquote><b>🌍 Which country do you want a number from for {platform}?</b></blockquote>"
        country_keyboard, no_countries = get_country_keyboard_for_platform(platform)
        if no_countries:
            country_text = f"<blockquote><b>😔 No numbers available for {platform} at the moment. Please try again later.</b></blockquote>"
        try:
            await query.edit_message_text(country_text, reply_markup=country_keyboard, parse_mode=ParseMode.HTML)
        except error.BadRequest:
            pass
        return

    elif query.data.startswith('user_country_'):
        if len(user_data.get('phone_numbers', [])) > 0:
            await query.answer("⚠️ You already have a number. Please delete it first.", show_alert=True)
            return

        flag = query.data.split('user_country_')[1]
        country_name = COUNTRIES.get(flag, "Unknown")
        
        # --- KEY FIX: Check for missing platform session data ---
        platform = context.user_data.get('selected_platform')
        if not platform:
             await query.answer("⚠️ Session expired. Please select platform again.", show_alert=True)
             try:
                 await query.message.delete()
             except:
                 pass
             await start_command(update, context)
             return
        
        # Check cooldown
        cooldown = 5
        last_time = user_data.get('last_number_time', 0)
        current_time = time.time()
        if current_time - last_time < cooldown:
            remaining_time = int(cooldown - (current_time - last_time))
            await query.answer(f"⚠️ Please wait {remaining_time} seconds.", show_alert=True)
            return

        number = await asyncio.to_thread(get_number_from_file_for_platform, country_name, platform)
        
        if not number:
            no_number_text = (
                "<blockquote><b>😔 Sorry!</b></blockquote>\n\n"
                f"<blockquote><b>No numbers available for {flag} {country_name} {platform} at the moment.</b>\n\n"
                "<blockquote><b>Please try other countries or platforms:</b></blockquote>"
            )
            try:
                await query.edit_message_text(no_number_text, reply_markup=get_main_menu_keyboard(), parse_mode=ParseMode.HTML)
            except error.BadRequest:
                pass
            return
        
        current_time = time.time()
        user_data["phone_numbers"].append(number)
        user_data["phone_numbers"] = user_data["phone_numbers"][-3:]
        user_data["last_number_time"] = current_time
        users_data[user_id] = user_data
        save_json_data(USERS_FILE, users_data)
        
        change_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("OTP GROUP", url=GROUP_LINK)],
            [InlineKeyboardButton("🗑️ Delete Number", callback_data=f"delete_number_{number}")],
            [InlineKeyboardButton("🔙 Back", callback_data='main_menu')]
        ])
        
        success_text = (
            "<blockquote><b>✅ Your new number is:</b></blockquote>\n\n"
            f"<blockquote><b>🌍 Country:</b> {flag} {country_name}</blockquote>\n\n"
            f"<blockquote><b>📱 Platform:</b> {platform}</blockquote>\n\n"
            f"<blockquote><b>📞 Number:</b> <code>{number}</code></blockquote>\n\n"
            "<blockquote><b>💡 Tips:</b></blockquote>\n\n"
            f"<blockquote><blockquote>• Use this number to register on {platform}</blockquote>\n\n"
            "<blockquote>• You will be notified automatically when SMS arrives</blockquote></blockquote>"
        )
        
        # Directly edit message with success text
        try:
            await query.edit_message_text(
                success_text,
                parse_mode=ParseMode.HTML,
                reply_markup=change_keyboard
            )
        except error.BadRequest:
            pass
        return
    
    elif query.data.startswith('country_'):
        flag = query.data.split('_')[1]
        country_name = COUNTRIES.get(flag, "Unknown")
        context.user_data['selected_country'] = country_name
        context.user_data['selected_flag'] = flag
        
        if user_id == str(ADMIN_ID):
            if context.user_data.get('state') == 'REMOVING_NUMBER':
                social_text = (
                    f"<blockquote><b>🗑️ Which social platform do you want to remove for {flag} {country_name}?</b></blockquote>\n\n"
                    "<blockquote><b>You can select multiple platforms:</b></blockquote>\n\n"
                    "<blockquote>• Select one or more platforms.</blockquote>\n\n"
                    "<blockquote>• Selected platforms will show a ✅ mark.</blockquote>\n\n"
                    "<blockquote>• Click 'Done - Continue' when finished.</blockquote>"
                )
            else:
                social_text = (
                    f"<blockquote><b>📱 Which social platform for {flag} {country_name}?</b></blockquote>\n\n"
                    "<blockquote><b>You can select multiple platforms:</b></blockquote>\n\n"
                    "<blockquote>• Select one or more platforms.</blockquote>\n\n"
                    "<blockquote>• Selected platforms will show a ✅ mark.</blockquote>\n\n"
                    "<blockquote>• Click 'Done - Continue' when finished.</blockquote>"
                )
        else:
            social_text = f"<blockquote><b>📱 Which social platform for {flag} {country_name}?</b></blockquote>"
        
        if user_id == str(ADMIN_ID):
            context.user_data['selected_platforms'] = set()
        
        try:
            await query.edit_message_text(
                social_text,
                reply_markup=get_admin_social_keyboard(),
                parse_mode=ParseMode.HTML
            )
        except error.BadRequest:
            pass
        return

    elif query.data.startswith('social_'):
        platform = query.data.split('_')[1]
        if user_id != str(ADMIN_ID):
            return

        country_name = context.user_data.get('selected_country', 'Unknown')
        flag = context.user_data.get('selected_flag', '🌍')
        
        if platform == 'done':
            selected_platforms = context.user_data.get('selected_platforms', set())
            if not selected_platforms:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="<blockquote><b>❌ Please select at least one platform.</b></blockquote>",
                    parse_mode=ParseMode.HTML
                )
                return
            
            if context.user_data.get('state') == 'REMOVING_NUMBER':
                platforms_text = ", ".join(sorted(selected_platforms))
                removed_count = remove_numbers_for_platforms(country_name, selected_platforms)
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"<blockquote><b>✅ Removed {removed_count} numbers for {flag} {country_name} from {platforms_text}!</b></blockquote>",
                    parse_mode=ParseMode.HTML
                )
                context.user_data['state'] = None
                context.user_data.pop('selected_country', None)
                context.user_data.pop('selected_flag', None)
                context.user_data.pop('selected_platforms', None)
            else:
                context.user_data['state'] = 'ADDING_NUMBER'
                context.user_data['selected_platforms'] = selected_platforms
                context.user_data['selected_country'] = country_name
                context.user_data['selected_flag'] = flag
                platforms_text = ", ".join(sorted(selected_platforms))
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"<blockquote><b>📞 Enter numbers for {flag} {country_name} for {platforms_text}:</b></blockquote>\n\n"
                         "<blockquote><b>Number format:</b> Digits only (8-15 digits)</blockquote>\n\n"
                         "<blockquote><b>Multiple numbers:</b> Write each number on a new line</blockquote>\n\n"
                         "<blockquote><b>Example:</b></blockquote>\n\n"
                         "<blockquote><code>1234567890\n"
                         "9876543210\n"
                         "5555555555</code></blockquote>\n\n"
                         "<blockquote><b>Type 'done' when finished</b></blockquote>",
                    parse_mode=ParseMode.HTML
                )
        else:
            selected_platforms = context.user_data.get('selected_platforms', set())
            if platform in selected_platforms:
                selected_platforms.remove(platform)
            else:
                selected_platforms.add(platform)
            context.user_data['selected_platforms'] = selected_platforms
            try:
                await query.edit_message_reply_markup(
                    reply_markup=get_admin_social_keyboard(selected_platforms)
                )
            except error.BadRequest:
                pass

    elif query.data.startswith('delete_number_'):
        number_to_delete = query.data.split('_')[2]
        
        users_data = load_json_data(USERS_FILE, {})
        user_data = users_data.get(user_id, {})
        
        if 'phone_numbers' in user_data and number_to_delete in user_data['phone_numbers']:
            user_data['phone_numbers'].remove(number_to_delete)
            users_data[user_id] = user_data
            save_json_data(USERS_FILE, users_data)
            
            await query.answer("🗑️ Number deleted successfully!", show_alert=True)
            try:
                await query.edit_message_text(
                    "<blockquote><b>🗑️ Number deleted successfully.</b></blockquote>\n\n"
                    "<blockquote>Click <b>🎁 Get Number</b> to get a new one.</blockquote>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None 
                )
            except error.BadRequest:
                pass
        else:
            await query.answer("❌ Number already deleted or not found.", show_alert=True)

    elif query.data.startswith('change_number_'):
        if len(user_data.get('phone_numbers', [])) > 0:
             await query.answer("⚠️ Please delete your current number first.", show_alert=True)
             return
             
        parts = query.data.split('_')
        if len(parts) >= 4:
            country_name = '_'.join(parts[2:-1])
            platform = parts[-1]
            
            cooldown = 5
            last_time = user_data.get('last_number_time', 0)
            current_time = time.time()
            if current_time - last_time < cooldown:
                remaining_time = int(cooldown - (current_time - last_time))
                await query.answer(f"⚠️ Please wait {remaining_time} seconds.", show_alert=True)
                return
            
            number = await asyncio.to_thread(get_number_from_file_for_platform, country_name, platform)
            if number:
                user_data["phone_numbers"].append(number)
                user_data["phone_numbers"] = user_data["phone_numbers"][-3:]
                user_data["last_number_time"] = current_time
                users_data[user_id] = user_data
                save_json_data(USERS_FILE, users_data)
                
                flag = "🌍"
                for f, c in COUNTRIES.items():
                    if c == country_name:
                        flag = f
                        break
                
                change_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("OTP GROUP", url=GROUP_LINK)],
                    [InlineKeyboardButton("🗑️ Delete Number", callback_data=f"delete_number_{number}")],
                    [InlineKeyboardButton("🔙 Back", callback_data='main_menu')]
                ])
                
                success_text = f"<blockquote><b>✅ Your new number is:</b></blockquote>\n\n" \
                               f"<blockquote><b>🌍 Country:</b> {flag} {country_name}</blockquote>\n\n" \
                               f"<blockquote><b>📱 Platform:</b> {platform}</blockquote>\n\n" \
                               f"<blockquote><b>📞 Number:</b> <code>{number}</code></blockquote>\n\n" \
                               f"<blockquote><b>OTP will be sent to your inbox.</b></blockquote>"
                               
                await MESSAGE_QUEUE.put({
                    'chat_id': user_id,
                    'text': success_text,
                    'parse_mode': ParseMode.HTML,
                    'reply_markup': change_keyboard
                })
                
                try:
                    await query.edit_message_text(
                        "<blockquote><b>✅ Your number has been sent successfully. Please check your inbox.</b></blockquote>",
                        parse_mode=ParseMode.HTML
                    )
                except error.BadRequest:
                    pass

            else:
                await context.bot.send_message(chat_id=user_id, text="<blockquote><b>😔 No numbers are available right now. Please try again later.</b></blockquote>", parse_mode=ParseMode.HTML)
                try:
                    await context.bot.send_message(chat_id=ADMIN_ID, text="<blockquote><b>⚠️ Admin Alert: The bot is out of numbers! Please add new numbers.</b></blockquote>", parse_mode=ParseMode.HTML)
                except Exception as e:
                    logging.error(f"Failed to send alert: {e}")

async def handle_add_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id != str(ADMIN_ID) or context.user_data.get('state') != 'ADDING_NUMBER':
        await start_command(update, context)
        return
    
    country = context.user_data.get('selected_country', 'Unknown')
    platforms = context.user_data.get('selected_platforms', set())
    
    if not platforms:
        await update.message.reply_text("<blockquote><b>❌ No platform selected. Please start over with /add.</b></blockquote>", parse_mode=ParseMode.HTML)
        context.user_data['state'] = None
        return
    
    numbers = update.message.text.split('\n')
    added_count = 0
    invalid_count = 0
    response_parts = []
    
    for number_str in numbers:
        number_str = number_str.strip()
        if number_str.lower() == 'done':
            break
        
        if 8 <= len(number_str) <= 15 and number_str.isdigit():
            for platform in platforms:
                await asyncio.to_thread(add_number_to_file, number_str, country, platform)
            added_count += 1
            response_parts.append(f"✅ <code>{number_str}</code> added")
        else:
            invalid_count += 1
            response_parts.append(f"❌ <code>{number_str}</code> invalid number")
    
    final_response = (
        f"<blockquote><b>✅ Number adding complete</b></blockquote>\n\n"
        f"<blockquote><b>Total added:</b> {added_count}</blockquote>\n\n"
        f"<blockquote><b>Invalid:</b> {invalid_count}</blockquote>\n\n"
    )
    
    if added_count > 0:
        final_response += "<b>Details:</b>\n\n" + "\n\n".join([f"<blockquote>{part}</blockquote>" for part in response_parts])
    
    if any(n.strip().lower() == 'done' for n in numbers):
        final_response += "\n\n<blockquote><b>✅ Number adding finished. Return to /start.</b></blockquote>"
        context.user_data['state'] = None
        context.user_data.pop('selected_country', None)
        context.user_data.pop('selected_flag', None)
        context.user_data.pop('selected_platforms', None)
    else:
        final_response += "\n\n<blockquote><b>...Enter more numbers or type 'done' to finish.</b></blockquote>"
    
    await update.message.reply_text(final_response, parse_mode=ParseMode.HTML)

async def handle_withdrawal_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get('state') != 'AWAITING_WITHDRAWAL_INFO':
        await start_command(update, context)
        return

    user_id = str(update.effective_user.id)
    payment_info = update.message.text
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id)
    
    if not user_data:
        return
        
    balance = user_data.get('balance', 0.0)
    
    if balance < WITHDRAWAL_LIMIT:
        await update.message.reply_text(
            f"<blockquote><b>❌ Insufficient Balance!</b></blockquote>\n\n"
            f"<blockquote>Minimum withdrawal is ${WITHDRAWAL_LIMIT}</blockquote>", 
            parse_mode=ParseMode.HTML
        )
        context.user_data['state'] = None
        return
    
    user_data['balance'] = 0.0
    users_data[user_id] = user_data
    save_json_data(USERS_FILE, users_data)
    
    context.user_data['state'] = None

    admin_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Approve", callback_data=f'admin_approve_{user_id}_{balance}'),
         InlineKeyboardButton("❌ Decline", callback_data=f'admin_decline_{user_id}_{balance}')]
    ])
    
    username = f"@{user_data.get('username')}" if user_data.get('username') else "N/A"
    admin_message = (
        f"<blockquote><b>🔥 New Withdrawal Request!</b></blockquote>\n\n"
        f"<blockquote><b>User:</b> {html_escape(user_data.get('first_name'))}</blockquote>\n\n"
        f"<blockquote><b>Username:</b> {username}</blockquote>\n\n"
        f"<blockquote><b>ID:</b> <code>{user_id}</code></blockquote>\n\n"
        f"<blockquote><b>Amount:</b> ${balance:.2f}</blockquote>\n\n"
        f"<blockquote><b>Payment Info:</b></blockquote>\n\n"
        f"<blockquote><code>{html_escape(payment_info)}</code></blockquote>"
    )
    
    try:
        await context.bot.send_message(
            chat_id=PAYMENT_CHANNEL_ID,
            text=admin_message,
            parse_mode=ParseMode.HTML,
            reply_markup=admin_keyboard
        )
    except Exception as e:
        logging.error(f"Failed to send to payment channel: {e}")
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=admin_message,
            parse_mode=ParseMode.HTML,
            reply_markup=admin_keyboard
        )

    await update.message.reply_text(
        f"<blockquote><b>✅ Withdrawal Request Submitted!</b></blockquote>\n\n"
        f"<blockquote>Your request for ${balance:.2f} is under review.</blockquote>", 
        parse_mode=ParseMode.HTML
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = context.user_data.get('state')
    text = update.message.text

    if state == 'ADDING_NUMBER':
        await handle_add_number(update, context)
    elif state == 'AWAITING_WITHDRAWAL_INFO':
        await handle_withdrawal_request(update, context)
    else:
        # Handle Keyboard Buttons
        if text == "🎁 Get Number":
            social_text = "<blockquote><b>📱 Which social platform do you want a number for?</b></blockquote>"
            social_keyboard = get_user_social_keyboard()
            if len(social_keyboard.inline_keyboard) == 1 and social_keyboard.inline_keyboard[0][0].text == "🔙 Back":
                social_text = "<blockquote><b>😔 No numbers available at the moment. Please try again later.</b></blockquote>"
            try:
                await update.message.reply_text(social_text, reply_markup=social_keyboard, parse_mode=ParseMode.HTML)
            except error.BadRequest:
                pass
        elif text == "👤 Account":
            user_id = str(update.effective_user.id)
            users_data = load_json_data(USERS_FILE, {})
            user_data = users_data.get(user_id)
            
            if not user_data:
                await update.message.reply_text("<blockquote>❌ Please type /start first.</blockquote>", parse_mode=ParseMode.HTML)
                return

            balance = user_data.get('balance', 0.0)
            account_text = (
                f"<blockquote><b>👤 Your Account</b></blockquote>\n\n"
                f"<blockquote><b>Name:</b> {html_escape(user_data.get('first_name'))}</blockquote>\n\n"
                f"<blockquote><b>User:</b> @{user_data.get('username', 'N/A')}</blockquote>\n\n"
                f"<blockquote><b>💰 Balance:</b> ${balance:.2f}</blockquote>"
            )
            keyboard = [
                [InlineKeyboardButton("💸 Withdraw", callback_data='withdraw')],
                [InlineKeyboardButton("🔙 Back", callback_data='main_menu')]
            ]
            try:
                await update.message.reply_text(account_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))
            except error.BadRequest:
                pass
        else:
            await start_command(update, context)

async def sms_watcher_task(application: Application):
    global manager_instance
    if not manager_instance:
        manager_instance = NewPanelSmsManager()
        
    while not shutdown_event.is_set():
        try:
            await asyncio.to_thread(manager_instance.scrape_and_save_all_sms)
            
            if not os.path.exists(SMS_CACHE_FILE):
                await asyncio.sleep(2)
                continue

            users_data = load_json_data(USERS_FILE, {})
            sent_sms_keys = load_sent_sms_keys()
            
            phone_to_user_map = {}
            for uid, udata in users_data.items():
                for number in udata.get("phone_numbers", []):
                    phone_to_user_map[number] = uid
            
            data_changed = False

            with open(SMS_CACHE_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        sms_data = json.loads(line)
                        phone = sms_data.get('phone')
                        country = sms_data.get('country', 'N/A')
                        provider = sms_data.get('provider', 'N/A')
                        message = sms_data.get('message')
                        otp = extract_otp_from_text(message)
                        
                        if otp == "N/A": continue

                        unique_key = f"{phone}|{otp}"
                        if unique_key in sent_sms_keys:
                            continue

                        number_country, number_platform, number_flag = get_number_info(phone)
                        
                        if not number_country:
                            detected_country, detected_flag = detect_country_from_phone(phone)
                            display_country = detected_country
                            display_flag = detected_flag
                        else:
                            display_country = number_country
                            display_flag = number_flag
                        
                        display_platform = number_platform if number_platform else provider
                        owner_id = phone_to_user_map.get(phone)
                        
                        group_keyboard = InlineKeyboardMarkup([
                            [InlineKeyboardButton("Number Bot", url="https://t.me/pgotp")]
                        ])
                        
                        is_instagram = "instagram" in provider.lower() or "instagram" in message.lower()
                        is_whatsapp = "whatsapp" in provider.lower() or "whatsapp" in message.lower()
                        
                        if is_instagram:
                            service_icon = "📸"
                            service_name = "Instagram"
                            service_display = "Instagram"
                            code_label = "Instagram Code"
                        elif is_whatsapp:
                            service_icon = "📱"
                            service_name = "WhatsApp"
                            service_display = "WhatsApp"
                            code_label = "WhatsApp Code"
                        else:
                            service_icon = "📱"
                            service_name = provider
                            service_display = "OTP"
                            code_label = "OTP Code"
                        
                        group_msg = (
                            f"{service_icon} <b>New {service_display}!</b> ✨\n\n"
                            f"📞 <b>Number:</b> <code>{hide_number(phone)}</code>\n\n"
                            f"🌍 <b>Country:</b> {html_escape(display_country)} {display_flag}\n\n"
                            f"🆔 <b>Service:</b> {html_escape(service_name)}\n\n"
                            f"🔑 <b>{code_label}:</b> <code>{otp}</code>\n\n"
                            f"📝 <b>Full Message:</b>\n\n"
                            f"<blockquote>{html_escape(message)}</blockquote>"
                        )

                        await MESSAGE_QUEUE.put({
                            'chat_id': GROUP_ID, 
                            'text': group_msg, 
                            'parse_mode': ParseMode.HTML, 
                            'reply_markup': group_keyboard
                        })

                        if owner_id:
                            if owner_id in users_data:
                                users_data[owner_id]['balance'] = users_data[owner_id].get('balance', 0.0) + SMS_AMOUNT
                                data_changed = True

                            inbox_keyboard = InlineKeyboardMarkup([
                                [InlineKeyboardButton("OTP GROUP", url=GROUP_LINK)]
                            ])
                            
                            inbox_msg = (
                                f"{service_icon} <b>New {service_display}!</b> ✨\n\n"
                                f"📞 <b>Number:</b> <code>{hide_number(phone)}</code>\n\n"
                                f"🌍 <b>Country:</b> {html_escape(display_country)} {display_flag}\n\n"
                                f"🆔 <b>Service:</b> {html_escape(service_name)}\n\n"
                                f"🔑 <b>{code_label}:</b> <code>{otp}</code>\n\n"
                                f"📝 <b>Full Message:</b>\n\n"
                                f"<blockquote>{html_escape(message)}</blockquote>\n\n"
                                f"<b>💰 Earned: ${SMS_AMOUNT}</b>"
                            )
                            
                            await MESSAGE_QUEUE.put({
                                'chat_id': owner_id, 
                                'text': inbox_msg, 
                                'parse_mode': ParseMode.HTML, 
                                'reply_markup': inbox_keyboard
                            })
                            
                            # Call the logging function here
                            asyncio.create_task(log_sms_to_d1({
                                "phone": phone,
                                "country": display_country,
                                "provider": service_name,
                                "message": message
                            }, otp, str(owner_id)))
                            
                            number_otp_key = f"{phone}_otp_received"
                            sent_sms_keys.add(number_otp_key)

                        sent_sms_keys.add(unique_key)

                    except Exception as e:
                        logging.error(f"Error processing SMS line: {e}")
            
            if data_changed:
                save_json_data(USERS_FILE, users_data)
                
            save_sent_sms_keys(sent_sms_keys)

        except Exception as e:
            logging.error(f"Error in sms_watcher_task: {e}")
        
        await asyncio.sleep(2)

async def test_group_access(application):
    try:
        test_msg = "<blockquote>🤖 Bot is now online and ready to receive SMS!</blockquote>"
        await application.bot.send_message(chat_id=GROUP_ID, text=test_msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Group access test FAILED: {e}")

async def main_bot_loop():
    global manager_instance
    try:
        load_config()
    except Exception as e:
        logging.critical(f"CRITICAL: Could not load config. {e}")
        return
        
    manager_instance = NewPanelSmsManager()
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("sms", sms_command))
    application.add_handler(CommandHandler("add", add_command))
    application.add_handler(CommandHandler("delete", delete_command))
    application.add_handler(CommandHandler("remove", delete_command))
    application.add_handler(CommandHandler("new", new_session_command))
    application.add_handler(CallbackQueryHandler(button_callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    await test_group_access(application)

    sms_task = asyncio.create_task(sms_watcher_task(application))
    sender_task = asyncio.create_task(rate_limited_sender_task(application))
    
    await shutdown_event.wait()
    
    sms_task.cancel()
    sender_task.cancel()
    try:
        await sms_task
        await sender_task
    except asyncio.CancelledError:
        pass

    await application.updater.stop()
    await application.stop()
    await application.shutdown()

if __name__ == "__main__":
    print("Starting bot...")
    try:
        asyncio.run(main_bot_loop())
    except KeyboardInterrupt:
        print("Bot shutting down manually...")
        shutdown_event.set()
    except Exception as e:
        logging.critical(f"Bot failed to start: {e}", exc_info=True)
        print(f"Bot failed to start: {e}")
