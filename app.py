import os
import json
import time
import random
import asyncio
import threading
from datetime import datetime, timezone, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, error
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode
import logging
import sys
import re
import configparser
import requests
from bs4 import BeautifulSoup
import telegram # Explicitly import full telegram module for sync helper function

# --- Configuration ---
CONFIG_FILE = 'config.txt'

def load_config():
    """
    Loads configuration from config.txt.
    The file MUST exist. The bot will not create a default one.
    """
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
    # Exit if config is missing or bad
    sys.exit(1)
    
# Bot and Panel Credentials
TELEGRAM_BOT_TOKEN = "8244964587:AAGLypfCDfQYDQZ3yw-OueUyVTUhILUcy9A"
GROUP_ID = -1002706128234
ADMIN_ID = 8012823221 # Admin ID for out-of-number notifications
# SUPPORT_IDS = ["@VIPSUPPORTC"]

# New Panel Credentials
PANEL_BASE_URL = "http://51.89.99.105/NumberPanel"
PANEL_SMS_URL = f"{PANEL_BASE_URL}/agent/SMSCDRStats"
# Prefer PHPSESSID from config if available
PHPSESSID = config.get('PHPSESSID', 'rpimjduka5o0bqp2hb3k1lrcp8')  # Session ID for API access

# Available Countries with flags (297 countries)
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
NUMBERS_FILE = 'numbers.txt' # File to store available numbers

# Global variables
shutdown_event = asyncio.Event()
manager_instance = None # Global instance for the SMS manager
MESSAGE_QUEUE = asyncio.Queue() # NEW: Queue for rate-limiting messages
LAST_SESSION_FAILURE_NOTIFICATION = 0 # NEW: Timestamp for last critical session failure notification

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
    # Rate limit this critical alert to once every 10 minutes (600 seconds)
    if time.time() - LAST_SESSION_FAILURE_NOTIFICATION < 600:
        return
        
    try:
        # Note: 'telegram' is imported at the top of the file
        sync_bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
        sync_bot.send_message(
            chat_id=ADMIN_ID, 
            text=f"<b>{message}</b>", 
            parse_mode=ParseMode.HTML
        )
        LAST_SESSION_FAILURE_NOTIFICATION = time.time()
    except Exception as e:
        # Logging here won't use the PTB logger, but standard Python logging
        logging.error(f"Failed to send critical admin notification: {e}")

def extract_otp_from_text(text):
    if not text: return "N/A"
    patterns = [
        # 1. Spaced or dashed 6-digit codes (Instagram, WhatsApp)
        r'Instagram.*?code\s*(\d{3}\s+\d{3})',  # Instagram code 758 431
        r'Instagram.*?(\d{3}\s+\d{3})',         # Instagram 758 431
        r'#\s*(\d{3}\s+\d{3})',                # # 758 431
        r'(\d{3}\s+\d{3})',                    # 758 431 format
        r'WhatsApp.*?code\s*(\d{3}-\d{3})',  # WhatsApp Business code 425-650
        r'WhatsApp.*?(\d{3}-\d{3})',        # WhatsApp code 425-650
        r'code\s*(\d{3}-\d{3})',            # code 425-650
        r'(\d{3}-\d{3})',                   # 425-650 format
        
        # 2. Specific prefixes / keywords
        r'G-(\d{6})', 
        r'code is\s*(\d+)', 
        r'code:\s*(\d+)', 
        r'verification code[:\s]*(\d+)', 
        r'OTP is\s*(\d+)', 
        r'pin[:\s]*(\d+)',
        
        # 3. New 8-digit cases (from user request)
        r'#\s*(\d{8})\b',                    # For: # 68133615 est votre...
        r'\b(\d{8})\b'                      # For general 8-digit codes
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            otp = match.group(1)
            # Handle Instagram format (758 431), WhatsApp format (425-650), and regular format
            if ' ' in otp and len(otp.replace(' ', '')) == 6:  # Instagram format
                return otp
            elif '-' in otp and len(otp) == 7:  # WhatsApp format
                return otp
            elif 4 <= len(otp) <= 8 and otp.isdigit():  # Regular format
                return otp
                
    # Fallback for 4-8 digit codes that weren't caught by specific patterns
    fallback_match = re.search(r'\b(\d{4,8})\b', text)
    return fallback_match.group(1) if fallback_match else "N/A"

def get_number_from_file_for_platform(country, platform):
    """Gets a number from numbers.txt file for specific country and platform, then deletes it."""
    if not os.path.exists(NUMBERS_FILE):
        return None
    
    with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    if not lines:
        return None
    
    # Find matching number for country and platform
    for i, line in enumerate(lines):
        try:
            # Try to parse as JSON (new format)
            number_info = json.loads(line)
            if (number_info.get("country") == country and 
                number_info.get("platform") == platform):
                # Found matching number, remove it from file
                number = number_info.get("number")
                if number:
                    # Remove this line from file
                    lines.pop(i)
                    with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
                        for remaining_line in lines:
                            f.write(remaining_line + "\n")
                    return number
        except:
            # Handle old format (plain numbers) - assume all are from Kenya
            if country == "Kenya":
                number = line
                if number:
                    # Remove this line from file
                    lines.pop(i)
                    with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
                        for remaining_line in lines:
                            f.write(remaining_line + "\n")
                    return number
    
    return None

def add_number_to_file(number, country=None, platform=None):
    """Adds a number to numbers.txt file with country and platform info with file locking."""
    import threading
    
    # Create a lock for file operations
    file_lock = threading.Lock()
    
    with file_lock:
        number_info = {
            "number": number,
            "country": country,
            "platform": platform,
            "added_date": get_bst_now().isoformat()
        }
        
        # Debug: Log what we're trying to add
        logging.error(f"Adding number to file: {number_info}")
        
        # Read existing content first
        existing_content = []
        if os.path.exists(NUMBERS_FILE):
            try:
                with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
                    existing_content = f.readlines()
            except Exception as e:
                logging.error(f"Error reading existing file: {e}")
                existing_content = []
        
        # Append new number
        try:
            with open(NUMBERS_FILE, 'a', encoding='utf-8') as f:
                f.write(json.dumps(number_info, ensure_ascii=False) + "\n")
            
            # Debug: Log success
            logging.error(f"Successfully added number {number} to file")
        except Exception as e:
            logging.error(f"Error writing to file: {e}")
            # Restore file if write failed
            try:
                with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
                    f.writelines(existing_content)
            except Exception as restore_error:
                logging.error(f"Failed to restore file: {restore_error}")

def remove_numbers_for_platforms(country, platforms):
    """Remove all numbers for specific country and platforms from numbers.txt."""
    import threading
    
    # Create a lock for file operations
    file_lock = threading.Lock()
    
    with file_lock:
        if not os.path.exists(NUMBERS_FILE):
            return 0
        
        # Read all lines
        try:
            with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception as e:
            logging.error(f"Error reading file for removal: {e}")
            return 0
        
        # Filter out lines that match country and platforms
        remaining_lines = []
        removed_count = 0
        
        for line in lines:
            line = line.strip()
            if not line:
                remaining_lines.append(line + "\n")
                continue
            
            try:
                # Try to parse as JSON (new format)
                number_info = json.loads(line)
                if (number_info.get("country") == country and 
                    number_info.get("platform") in platforms):
                    removed_count += 1
                    logging.error(f"Removed number: {number_info}")
                else:
                    remaining_lines.append(line + "\n")
            except:
                # Handle old format (plain numbers) - assume all are from Kenya
                if country == "Kenya":
                    removed_count += 1
                    logging.error(f"Removed old format number: {line}")
                else:
                    remaining_lines.append(line + "\n")
        
        # Write remaining lines back to file
        try:
            with open(NUMBERS_FILE, 'w', encoding='utf-8') as f:
                f.writelines(remaining_lines)
            logging.error(f"Successfully removed {removed_count} numbers from file")
        except Exception as e:
            logging.error(f"Error writing file after removal: {e}")
            return 0
        
        return removed_count

def get_available_countries_for_platform(platform):
    """Returns list of countries that have numbers available for a specific platform."""
    if not os.path.exists(NUMBERS_FILE):
        return []
    
    countries = set()
    with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                # Try to parse as JSON (new format)
                number_info = json.loads(line)
                if number_info.get("platform") == platform:
                    country = number_info.get("country")
                    if country:
                        countries.add(country)
            except:
                # Handle old format (plain numbers) - assume all are from Kenya
                if platform in ["WhatsApp", "Facebook", "Instagram"]:  # Common platforms
                    countries.add("Kenya")
    
    return list(countries)

def get_number_count_for_country_and_platform(country, platform):
    """Returns the number of available numbers for a specific country and platform."""
    if not os.path.exists(NUMBERS_FILE):
        return 0
    
    count = 0
    with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                # Try to parse as JSON (new format)
                number_info = json.loads(line)
                if (number_info.get("country") == country and 
                    number_info.get("platform") == platform):
                    count += 1
            except:
                # Handle old format (plain numbers) - assume all are from Kenya
                if country == "Kenya" and platform in ["WhatsApp", "Facebook", "Instagram"]:
                    count += 1
    
    return count

def get_available_countries():
    """Returns list of countries that have available numbers."""
    if not os.path.exists(NUMBERS_FILE):
        return []
    
    countries = set()
    with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                # Try to parse as JSON (new format)
                number_info = json.loads(line)
                country = number_info.get("country")
                if country:
                    countries.add(country)
            except:
                # Handle old format (plain numbers) - assume all are from a default country
                # You can change this default country as needed
                countries.add("Kenya")  # Default country for old format numbers
    
    return list(countries)

def get_number_count_for_country(country):
    """Returns the number of available numbers for a specific country."""
    if not os.path.exists(NUMBERS_FILE):
        return 0
    
    count = 0
    with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                # Try to parse as JSON (new format)
                number_info = json.loads(line)
                if number_info.get("country") == country:
                    count += 1
            except:
                # Handle old format (plain numbers) - assume all are from Kenya
                if country == "Kenya":
                    count += 1
    
    return count

def get_available_platforms_for_country(country):
    """Returns list of platforms that have available numbers for the given country."""
    if not os.path.exists(NUMBERS_FILE):
        return []
    
    platforms = set()
    with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                # Try to parse as JSON (new format)
                number_info = json.loads(line)
                if number_info.get("country") == country:
                    platform = number_info.get("platform")
                    if platform:
                        platforms.add(platform)
            except:
                # Handle old format (plain numbers) - assume all are for a default platform
                # You can change this default platform as needed
                platforms.add("WhatsApp")  # Default platform for old format numbers
    
    return list(platforms)

def get_admin_country_keyboard(page=0):
    """Creates a paginated keyboard for admin country selection showing all countries."""
    keyboard = []
    countries_list = list(COUNTRIES.items())
    items_per_page = 80  # 40 rows of 2 buttons

    start_index = page * items_per_page
    end_index = start_index + items_per_page
    
    paginated_countries = countries_list[start_index:end_index]

    # Create rows with 2 countries each
    for i in range(0, len(paginated_countries), 2):
        row = []
        for j in range(2):
            if i + j < len(paginated_countries):
                flag, country = paginated_countries[i + j]
                row.append(InlineKeyboardButton(f"{flag} {country}", callback_data=f"country_{flag}"))
        keyboard.append(row)
    
    # Pagination controls
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
    """Creates keyboard for admin social platform selection showing all platforms."""
    if selected_platforms is None:
        selected_platforms = set()
    
    keyboard = []
    
    # Create rows with 2 platforms each
    for i in range(0, len(SOCIAL_PLATFORMS), 2):
        row = []
        for j in range(2):
            if i + j < len(SOCIAL_PLATFORMS):
                platform = SOCIAL_PLATFORMS[i + j]
                # Add checkmark if platform is selected
                button_text = f"✅ {platform}" if platform in selected_platforms else platform
                row.append(InlineKeyboardButton(button_text, callback_data=f"social_{platform}"))
        keyboard.append(row)
    
    # Add done button if platforms are selected
    if selected_platforms:
        keyboard.append([InlineKeyboardButton("✅ Done - Continue", callback_data="social_done")])
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard)

def get_user_social_keyboard():
    """Creates keyboard for social platform selection for users - shows only platforms with available numbers."""
    keyboard = []
    
    # Filter platforms to only show those with available numbers
    available_platforms = []
    for platform in SOCIAL_PLATFORMS:
        available_countries = get_available_countries_for_platform(platform)
        if available_countries:  # Only show platforms that have numbers
            available_platforms.append(platform)
    
    if not available_platforms:
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]
        return InlineKeyboardMarkup(keyboard)
    
    # Create rows with 2 platforms each
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
    """Creates keyboard for country selection for a specific platform."""
    available_countries = get_available_countries_for_platform(platform)
    
    if not available_countries:
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]
        return InlineKeyboardMarkup(keyboard), True  # Return True to indicate no countries available
    
    keyboard = []
    countries_list = []
    
    # Filter COUNTRIES to only show available ones for this platform and get count for each
    for flag, country in COUNTRIES.items():
        if country in available_countries:
            count = get_number_count_for_country_and_platform(country, platform)
            if count > 0:  # Only show countries that have numbers for this platform
                countries_list.append((flag, country, count))
    
    # Create rows with 2 countries each
    for i in range(0, len(countries_list), 2):
        row = []
        for j in range(2):
            if i + j < len(countries_list):
                flag, country, count = countries_list[i + j]
                row.append(InlineKeyboardButton(f"{flag} {country} ({count})", callback_data=f"user_country_{flag}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard), False  # Return False to indicate countries are available

def get_number_info(phone_number):
    """Get country and platform info for a phone number from numbers.txt."""
    if not os.path.exists(NUMBERS_FILE):
        return None, None, None
    
    with open(NUMBERS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                # Try to parse as JSON (new format)
                number_info = json.loads(line)
                if number_info.get("number") == phone_number:
                    country = number_info.get("country")
                    platform = number_info.get("platform")
                    # Get flag for country
                    flag = None
                    if country:
                        for f, c in COUNTRIES.items():
                            if c == country:
                                flag = f
                                break
                    return country, platform, flag
            except:
                # Handle old format (plain numbers)
                if line == phone_number:
                    # Return default country and platform for old format
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
    """Detect country from phone number prefix"""
    if not phone:
        return "Unknown", "🌍"
    
    phone_str = str(phone).replace("+", "").replace(" ", "").replace("-", "")
    
    # Country code mappings (most common prefixes)
    country_codes = {
        "1": ("United States", "🇺🇸"),
        "7": ("Russia", "🇷🇺"),
        "20": ("Egypt", "🇪🇬"),
        "27": ("South Africa", "🇿🇦"),
        "30": ("Greece", "🇬🇷"),
        "31": ("Netherlands", "🇳🇱"),
        "32": ("Belgium", "🇧🇪"),
        "33": ("France", "🇫🇷"),
        "34": ("Spain", "🇪🇸"),
        "36": ("Hungary", "🇭🇺"),
        "39": ("Italy", "🇮🇹"),
        "40": ("Romania", "🇷🇴"),
        "41": ("Switzerland", "🇨🇭"),
        "43": ("Austria", "🇦🇹"),
        "44": ("United Kingdom", "🇬🇧"),
        "45": ("Denmark", "🇩🇰"),
        "46": ("Sweden", "🇸🇪"),
        "47": ("Norway", "🇳🇴"),
        "48": ("Poland", "🇵🇱"),
        "49": ("Germany", "🇩🇪"),
        "51": ("Peru", "🇵🇪"),
        "52": ("Mexico", "🇲🇽"),
        "53": ("Cuba", "🇨🇺"),
        "54": ("Argentina", "🇦🇷"),
        "55": ("Brazil", "🇧🇷"),
        "56": ("Chile", "🇨🇱"),
        "57": ("Colombia", "🇨🇴"),
        "58": ("Venezuela", "🇻🇪"),
        "60": ("Malaysia", "🇲🇾"),
        "61": ("Australia", "🇦🇺"),
        "62": ("Indonesia", "🇮🇩"),
        "63": ("Philippines", "🇵🇭"),
        "64": ("New Zealand", "🇳🇿"),
        "65": ("Singapore", "🇸🇬"),
        "66": ("Thailand", "🇹🇭"),
        "81": ("Japan", "🇯🇵"),
        "82": ("South Korea", "🇰🇷"),
        "84": ("Vietnam", "🇻🇳"),
        "86": ("China", "🇨🇳"),
        "90": ("Turkey", "🇹🇷"),
        "91": ("India", "🇮🇳"),
        "92": ("Pakistan", "🇵🇰"),
        "93": ("Afghanistan", "🇦🇫"),
        "94": ("Sri Lanka", "🇱🇰"),
        "95": ("Myanmar", "🇲🇲"),
        "98": ("Iran", "🇮🇷"),
        "212": ("Morocco", "🇲🇦"),
        "213": ("Algeria", "🇩🇿"),
        "216": ("Tunisia", "🇹🇳"),
        "218": ("Libya", "🇱🇾"),
        "220": ("Gambia", "🇬🇲"),
        "221": ("Senegal", "🇸🇳"),
        "222": ("Mauritania", "🇲🇷"),
        "223": ("Mali", "🇲🇱"),
        "224": ("Guinea", "🇬🇳"),
        "225": ("Ivory Coast", "🇨🇮"),
        "226": ("Burkina Faso", "🇧🇫"),
        "227": ("Niger", "🇳🇪"),
        "228": ("Togo", "🇹🇬"),
        "229": ("Benin", "🇧🇯"),
        "230": ("Mauritius", "🇲🇺"),
        "231": ("Liberia", "🇱🇷"),
        "232": ("Sierra Leone", "🇸🇱"),
        "233": ("Ghana", "🇬🇭"),
        "234": ("Nigeria", "🇳🇬"),
        "235": ("Chad", "🇹🇩"),
        "236": ("Central African Republic", "🇨🇫"),
        "237": ("Cameroon", "🇨🇲"),
        "238": ("Cape Verde", "🇨🇻"),
        "239": ("Sao Tome and Principe", "🇸🇹"),
        "240": ("Equatorial Guinea", "🇬🇶"),
        "241": ("Gabon", "🇬🇦"),
        "242": ("Congo", "🇨🇬"),
        "243": ("Congo", "🇨🇩"),
        "244": ("Angola", "🇦🇴"),
        "245": ("Guinea-Bissau", "🇬🇼"),
        "246": ("British Indian Ocean Territory", "🇮🇴"),
        "248": ("Seychelles", "🇸🇨"),
        "249": ("Sudan", "🇸🇩"),
        "250": ("Rwanda", "🇷🇼"),
        "251": ("Ethiopia", "🇪🇹"),
        "252": ("Somalia", "🇸🇴"),
        "253": ("Djibouti", "🇩🇯"),
        "254": ("Kenya", "🇰🇪"),
        "255": ("Tanzania", "🇹🇿"),
        "256": ("Uganda", "🇺🇬"),
        "257": ("Burundi", "🇧🇮"),
        "258": ("Mozambique", "🇲🇿"),
        "260": ("Zambia", "🇿🇲"),
        "261": ("Madagascar", "🇲🇬"),
        "262": ("Reunion", "🇷🇪"),
        "263": ("Zimbabwe", "🇿🇼"),
        "264": ("Namibia", "🇳🇦"),
        "265": ("Malawi", "🇲🇼"),
        "266": ("Lesotho", "🇱🇸"),
        "267": ("Botswana", "🇧🇼"),
        "268": ("Eswatini", "🇸🇿"),
        "269": ("Comoros", "🇰🇲"),
        "290": ("Saint Helena", "🇸🇭"),
        "291": ("Eritrea", "🇪🇷"),
        "297": ("Aruba", "🇦🇼"),
        "298": ("Faroe Islands", "🇫🇴"),
        "299": ("Greenland", "🇬🇱"),
        "350": ("Gibraltar", "🇬🇮"),
        "351": ("Portugal", "🇵🇹"),
        "352": ("Luxembourg", "🇱🇺"),
        "353": ("Ireland", "🇮🇪"),
        "354": ("Iceland", "🇮🇸"),
        "355": ("Albania", "🇦🇱"),
        "356": ("Malta", "🇲🇹"),
        "357": ("Cyprus", "🇨🇾"),
        "358": ("Finland", "🇫🇮"),
        "359": ("Bulgaria", "🇧🇬"),
        "370": ("Lithuania", "🇱🇹"),
        "371": ("Latvia", "🇱🇻"),
        "372": ("Estonia", "🇪🇪"),
        "373": ("Moldova", "🇲🇩"),
        "374": ("Armenia", "🇦🇲"),
        "375": ("Belarus", "🇧🇾"),
        "376": ("Andorra", "🇦🇩"),
        "377": ("Monaco", "🇲🇨"),
        "378": ("San Marino", "🇸🇲"),
        "380": ("Ukraine", "🇺🇦"),
        "381": ("Serbia", "🇷🇸"),
        "382": ("Montenegro", "🇲🇪"),
        "383": ("Kosovo", "🇽🇰"),
        "385": ("Croatia", "🇭🇷"),
        "386": ("Slovenia", "🇸🇮"),
        "387": ("Bosnia and Herzegovina", "🇧🇦"),
        "389": ("North Macedonia", "🇲🇰"),
        "420": ("Czech Republic", "🇨🇿"),
        "421": ("Slovakia", "🇸🇰"),
        "423": ("Liechtenstein", "🇱🇮"),
        "500": ("Falkland Islands", "🇫🇰"),
        "501": ("Belize", "🇧🇿"),
        "502": ("Guatemala", "🇬🇹"),
        "503": ("El Salvador", "🇸🇻"),
        "504": ("Honduras", "🇭🇳"),
        "505": ("Nicaragua", "🇳🇮"),
        "506": ("Costa Rica", "🇨🇷"),
        "507": ("Panama", "🇵🇦"),
        "508": ("Saint Pierre and Miquelon", "🇵🇲"),
        "509": ("Haiti", "🇭🇹"),
        "590": ("Guadeloupe", "🇬🇵"),
        "591": ("Bolivia", "🇧🇴"),
        "592": ("Guyana", "🇬🇾"),
        "593": ("Ecuador", "🇪🇨"),
        "594": ("French Guiana", "🇬🇫"),
        "595": ("Paraguay", "🇵🇾"),
        "596": ("Martinique", "🇲🇶"),
        "597": ("Suriname", "🇸🇷"),
        "598": ("Uruguay", "🇺🇾"),
        "599": ("Netherlands Antilles", "🇳🇱"),
        "670": ("Timor-Leste", "🇹🇱"),
        "672": ("Australian External Territories", "🇦🇺"),
        "673": ("Brunei", "🇧🇳"),
        "674": ("Nauru", "🇳🇷"),
        "675": ("Papua New Guinea", "🇵🇬"),
        "676": ("Tonga", "🇹🇴"),
        "677": ("Solomon Islands", "🇸🇧"),
        "678": ("Vanuatu", "🇻🇺"),
        "679": ("Fiji", "🇫🇯"),
        "680": ("Palau", "🇵🇼"),
        "681": ("Wallis and Futuna", "🇼🇫"),
        "682": ("Cook Islands", "🇨🇰"),
        "683": ("Niue", "🇳🇺"),
        "684": ("American Samoa", "🇦🇸"),
        "685": ("Samoa", "🇼🇸"),
        "686": ("Kiribati", "🇰🇮"),
        "687": ("New Caledonia", "🇳🇨"),
        "688": ("Tuvalu", "🇹🇻"),
        "689": ("French Polynesia", "🇵🇫"),
        "690": ("Tokelau", "🇹🇰"),
        "691": ("Micronesia", "🇫🇲"),
        "692": ("Marshall Islands", "🇲🇭"),
        "850": ("North Korea", "🇰🇵"),
        "852": ("Hong Kong", "🇭🇰"),
        "853": ("Macau", "🇲🇴"),
        "855": ("Cambodia", "🇰🇭"),
        "856": ("Laos", "🇱🇦"),
        "880": ("Bangladesh", "🇧🇩"),
        "886": ("Taiwan", "🇹🇼"),
        "960": ("Maldives", "🇲🇻"),
        "961": ("Lebanon", "🇱🇧"),
        "962": ("Jordan", "🇯🇴"),
        "963": ("Syria", "🇸🇾"),
        "964": ("Iraq", "🇮🇶"),
        "965": ("Kuwait", "🇰🇼"),
        "966": ("Saudi Arabia", "🇸🇦"),
        "967": ("Yemen", "🇾🇪"),
        "968": ("Oman", "🇴🇲"),
        "970": ("Palestine", "🇵🇸"),
        "971": ("United Arab Emirates", "🇦🇪"),
        "972": ("Israel", "🇮🇱"),
        "973": ("Bahrain", "🇧🇭"),
        "974": ("Qatar", "🇶🇦"),
        "975": ("Bhutan", "🇧🇹"),
        "976": ("Mongolia", "🇲🇳"),
        "977": ("Nepal", "🇳🇵"),
        "992": ("Tajikistan", "🇹🇯"),
        "993": ("Turkmenistan", "🇹🇲"),
        "994": ("Azerbaijan", "🇦🇿"),
        "995": ("Georgia", "🇬🇪"),
        "996": ("Kyrgyzstan", "🇰🇬"),
        "998": ("Uzbekistan", "🇺🇿"),
    }
    
    # Try different prefix lengths (longest first)
    for length in [3, 2, 1]:
        if len(phone_str) >= length:
            prefix = phone_str[:length]
            if prefix in country_codes:
                return country_codes[prefix]
    
    return "Unknown", "🌍"

# --- Modern API-based SMS Manager Class ---
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
        """Initialize API-based SMS fetching"""
        self._is_initialized = True
        logging.info("API-based SMS manager initialized")
    
    def get_api_url(self):
        """Get API URL for SMS data"""
        today = datetime.now().strftime("%Y-%m-%d")
        # wider length to reduce pagination misses
        return f"{PANEL_BASE_URL}/agent/res/data_smscdr.php?fdate1={today}+00:00:00&fdate2={today}+23:59:59&iDisplayLength=200"
    
    def fetch_sms_from_api(self):
        """Fetch SMS data by getting structured JSON from the dedicated data endpoint."""
        # 1. Check session validity by hitting the main HTML page
        session_check_headers = {
            "cookie": f"PHPSESSID={PHPSESSID}",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 OPR/122.0.0.0"
        }
        try:
            # Hit the /SMSCDRStats page to check for login redirect
            html_resp = requests.get(PANEL_SMS_URL, headers=session_check_headers, timeout=10)
            html_resp.raise_for_status()
            soup = BeautifulSoup(html_resp.text, "html.parser")
            title_tag = soup.find('title')
            
            # CRITICAL SESSION CHECK: If we see the login page, session is bad.
            if title_tag and 'Login' in title_tag.get_text():
                logging.error("Session check: appears to be login page. Update PHPSESSID.")
                error_msg = f"🚨 CRITICAL: Panel Session Expired! Update PHPSESSID in config.txt IMMEDIATELY. Time: {get_bst_now().strftime('%H:%M:%S')} BST"
                _send_critical_admin_alert(error_msg)
                return []
        except Exception as e:
            logging.warning(f"Initial session check failed: {e}")
            return [] # Cannot proceed without a valid session or check

        # 2. Fetch SMS data from the structured JSON endpoint
        data_url = self.get_api_url()
        data_headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "x-requested-with": "XMLHttpRequest", # Common AJAX header
            "cookie": f"PHPSESSID={PHPSESSID}",
            "referer": f"{PANEL_BASE_URL}/agent/SMSDashboard", # Required referer for the data endpoint
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 OPR/122.0.0.0"
        }
        
        retries = 3
        for attempt in range(retries):
            try:
                data_resp = requests.get(data_url, headers=data_headers, timeout=10)
                data_resp.raise_for_status()
                
                # Datatables often returns JSON with 'aaData' key
                json_data = data_resp.json()
                
                if 'aaData' in json_data and isinstance(json_data['aaData'], list):
                    return json_data['aaData']
                elif isinstance(json_data, list):
                    return json_data # Sometimes it returns the array directly

                logging.warning(f"Data fetch attempt {attempt + 1}/{retries}: JSON missing 'aaData' or unexpected format.")
                
            except json.JSONDecodeError:
                logging.error(f"Data fetch attempt {attempt + 1}/{retries}: Response is not valid JSON. Response Text: {data_resp.text[:200]}...")
            except Exception as data_err:
                logging.warning(f"Data fetch attempt {attempt + 1}/{retries} failed: {data_err}")
                
            if attempt < retries - 1:
                time.sleep(5)
        
        logging.error("SMS data fetch failed after all attempts.")
        return []

    def scrape_and_save_all_sms(self):
        """Fetch SMS from API and save to file"""
        try:
            # This now calls the modified fetch_sms_from_api which returns structured data
            sms_data = self.fetch_sms_from_api()
            logging.info(f"Fetched {len(sms_data)} rows from API.") # Added logging
            sms_list = []
            
            for row in sms_data:
                try:
                    # NOTE: Row indexing remains consistent with the Datatables JSON response format:
                    # [time, country/provider, number, service, status, message]
                    if len(row) >= 6:
                        # Extract data from API response
                        time_str = row[0] if len(row) > 0 else "N/A"
                        country_provider = row[1] if len(row) > 1 else "Unknown"
                        phone = row[2] if len(row) > 2 else "N/A"
                        service = row[3] if len(row) > 3 else "Unknown Service"
                        message = row[5] if len(row) > 5 else "N/A"
                        
                        # Extract country from country_provider string
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

            logging.info(f"Processed {len(sms_list)} valid SMS entries.") # Added logging
            # Save SMS to file
            with self._lock:
                with open(SMS_CACHE_FILE, 'w', encoding='utf-8') as f:
                    for sms in sms_list:
                        f.write(json.dumps(sms) + "\n")
            
        except Exception as e:
            logging.error(f"SMS API fetch failed: {e}")

    def cleanup(self):
        """Cleanup method for compatibility"""
        pass

# --- Rate-Limited Sender Task (NEW) ---
async def rate_limited_sender_task(application: Application):
    """Pulls messages from the queue and sends them with rate limiting."""
    while not shutdown_event.is_set():
        try:
            # Wait for a message in the queue
            message_data = await MESSAGE_QUEUE.get()
            
            chat_id = message_data['chat_id']
            text = message_data['text']
            parse_mode = message_data.get('parse_mode', ParseMode.HTML)
            reply_markup = message_data.get('reply_markup')
            
            # Use a safe retry loop for sending to handle RetryAfter
            retry_attempts = 5
            for attempt in range(retry_attempts):
                try:
                    await application.bot.send_message(
                        chat_id=chat_id, 
                        text=text, 
                        parse_mode=parse_mode, 
                        reply_markup=reply_markup
                    )
                    break # Success
                except error.RetryAfter as e:
                    sleep_time = e.retry_after + 1
                    logging.warning(f"Telegram rate limit hit. Sleeping for {sleep_time}s. Chat ID: {chat_id}")
                    await asyncio.sleep(sleep_time)
                except Exception as e:
                    logging.error(f"Failed to send message to {chat_id} (Attempt {attempt+1}/{retry_attempts}): {e}")
                    if attempt == retry_attempts - 1:
                        logging.error(f"Giving up on message to {chat_id} after all retries.")
                    else:
                        await asyncio.sleep(2) # Small delay before trying again
            
            MESSAGE_QUEUE.task_done()
            
            # Global rate limit delay (stay well below 30 msg/sec, 0.05s = 20 msg/sec)
            await asyncio.sleep(0.05) 
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"Error in rate_limited_sender_task: {e}")
            await asyncio.sleep(1) # Sleep to prevent tight loop on error

# --- Telegram Bot UI and Logic ---

def get_main_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("🎁 Get Number", callback_data='get_number')]
    ]
    return InlineKeyboardMarkup(keyboard)

async def sms_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /sms command to show SMS information"""
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
    
    for i, number in enumerate(phone_numbers[:5], 1):  # Show max 5 numbers
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
    """Handle /add command for admin to add numbers."""
    user_id = str(update.effective_user.id)
    
    # Check if user is admin
    if user_id != str(ADMIN_ID):
        await update.message.reply_text("<blockquote><b>❌ This command can only be used by admin.</b></blockquote>", parse_mode=ParseMode.HTML)
        return
    
    # Show country selection
    country_text = (
        "<blockquote><b>🌍 Which country's numbers do you want to add? (Page 1)</b></blockquote>"
    )
    
    await update.message.reply_text(
        country_text,
        reply_markup=get_admin_country_keyboard(page=0),
        parse_mode=ParseMode.HTML
    )

async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /delete or /remove command for admin to remove numbers."""
    user_id = str(update.effective_user.id)
    
    # Check if user is admin
    if user_id != str(ADMIN_ID):
        await update.message.reply_text("<blockquote><b>❌ This command can only be used by admin.</b></blockquote>", parse_mode=ParseMode.HTML)
        return
    
    # Show country selection for removal
    country_text = (
        "<blockquote><b>🗑️ Which country's numbers do you want to remove? (Page 1)</b></blockquote>"
    )
    
    # Set state for removal
    context.user_data['state'] = 'REMOVING_NUMBER'
    
    await update.message.reply_text(
        country_text,
        reply_markup=get_admin_country_keyboard(page=0),
        parse_mode=ParseMode.HTML
    )

async def new_session_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /new command for admin to update PHPSESSID."""
    global PHPSESSID # Declare we are modifying the global variable
    user_id = str(update.effective_user.id)
    
    # Check if user is admin
    if user_id != str(ADMIN_ID):
        await update.message.reply_text("<blockquote><b>❌ This command can only be used by admin.</b></blockquote>", parse_mode=ParseMode.HTML)
        return

    # Check for argument
    if not context.args:
        await update.message.reply_text(
            "<blockquote><b>Usage:</b> /new &lt;NEW_PHPSESSID&gt;</blockquote>\n\n"
            "<blockquote><b>Example:</b> /new abc123def456...</blockquote>",
            parse_mode=ParseMode.HTML
        )
        return
        
    new_session_id = context.args[0]
    
    try:
        # 1. Update config.txt
        config_parser = configparser.ConfigParser()
        # Read with utf-8 to preserve file integrity
        config_parser.read(CONFIG_FILE, encoding='utf-8')
        
        if 'Settings' not in config_parser:
            config_parser['Settings'] = {}
            
        config_parser['Settings']['PHPSESSID'] = new_session_id
        
        with open(CONFIG_FILE, 'w', encoding='utf-8') as configfile:
            config_parser.write(configfile)
            
        # 2. Update the global variable in memory
        PHPSESSID = new_session_id
        
        # 3. Notify admin
        await update.message.reply_text(
            f"<blockquote><b>✅ PHPSESSID updated successfully!</b></blockquote>\n\n"
            f"<blockquote><b>New ID:</b> <code>{html_escape(new_session_id)}</code></blockquote>",
            parse_mode=ParseMode.HTML
        )
        logging.error(f"Admin updated PHPSESSID to: {new_session_id}")

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
            "username": user.username, "first_name": user.first_name, "phone_numbers": [],
            "last_number_time": 0
        }
    
    save_json_data(USERS_FILE, users_data)
    
    welcome_text = (
        "<blockquote><b>👋 Welcome!</b></blockquote>\n\n"
        "<blockquote>Click the 🎁 Get Number button below to get your number:</blockquote>\n\n"
        # "<b>Support Inbox 📤</b>\n"
        # f"<blockquote>👉 {SUPPORT_IDS[0]}</blockquote>\n\n"
    )

    if update.callback_query:
        await update.callback_query.edit_message_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(), parse_mode=ParseMode.HTML)

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except error.BadRequest as e:
        logging.warning(f"Could not answer callback query (likely old): {e}")

    user_id = str(query.from_user.id)
    users_data = load_json_data(USERS_FILE, {})
    user_data = users_data.get(user_id)

    back_button = [InlineKeyboardButton("🔙 Back", callback_data='main_menu')]

    if not user_data:
        try:
            await query.edit_message_text("<blockquote>❌ An error occurred.</blockquote>\n\n<blockquote>Please restart with /start command.</blockquote>", parse_mode=ParseMode.HTML)
        except error.BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Ignore this error - message is already the same
            else:
                logging.error(f"Failed to edit message: {e}")
        return
        
    if query.data == 'main_menu':
        try:
            await start_command(update, context)
        except error.BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Ignore this error - message is already the same
            else:
                logging.error(f"Failed to edit message: {e}")
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
        except error.BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                logging.error(f"Failed to edit message: {e}")
        return

    if query.data == 'check_membership':
        # Re-check membership and redirect to start command
        try:
            await start_command(update, context)
        except error.BadRequest as e:
            if "Message is not modified" in str(e):
                pass  # Ignore this error - message is already the same
            else:
                logging.error(f"Failed to edit message: {e}")
        return

    if query.data == 'get_number':
        # Show social platform selection first
        social_text = "<blockquote><b>📱 Which social platform do you want a number for?</b></blockquote>"
        
        social_keyboard = get_user_social_keyboard()
        
        # Check if no platforms are available
        if len(social_keyboard.inline_keyboard) == 1 and social_keyboard.inline_keyboard[0][0].text == "🔙 Back":
            social_text = "<blockquote><b>😔 No numbers available at the moment. Please try again later.</b></blockquote>"
        
        try:
            await query.edit_message_text(
                social_text,
                reply_markup=social_keyboard,
                parse_mode=ParseMode.HTML
            )
        except error.BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                logging.error(f"Failed to edit message: {e}")
        return

    elif query.data.startswith('user_social_'):
        platform = query.data.split('user_social_')[1]
        
        # Store selected platform in context
        context.user_data['selected_platform'] = platform
        
        # Show country selection for this platform
        country_text = f"<blockquote><b>🌍 Which country do you want a number from for {platform}?</b></blockquote>"
        
        country_keyboard, no_countries = get_country_keyboard_for_platform(platform)
        
        if no_countries:
            # Show no numbers available message
            country_text = f"<blockquote><b>😔 No numbers available for {platform} at the moment. Please try again later.</b></blockquote>"
        
        try:
            await query.edit_message_text(
                country_text,
                reply_markup=country_keyboard,
                parse_mode=ParseMode.HTML
            )
        except error.BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                logging.error(f"Failed to edit message: {e}")
        return

    elif query.data.startswith('user_country_'):
        flag = query.data.split('user_country_')[1]
        country_name = COUNTRIES.get(flag, "Unknown")
        platform = context.user_data.get('selected_platform', 'Unknown')
        
        # Get number for this platform and country
        number = await asyncio.to_thread(get_number_from_file_for_platform, country_name, platform)
        
        if not number:
            # No number available
            no_number_text = (
                "<blockquote><b>😔 Sorry!</b></blockquote>\n\n"
                f"<blockquote><b>No numbers available for {flag} {country_name} {platform} at the moment.</b></blockquote>\n\n"
                "<blockquote><b>Please try other countries or platforms:</b></blockquote>"
            )
            
            try:
                await query.edit_message_text(
                    no_number_text,
                    reply_markup=get_main_menu_keyboard(),
                    parse_mode=ParseMode.HTML
                )
            except error.BadRequest as e:
                if "Message is not modified" in str(e):
                    pass
                else:
                    logging.error(f"Failed to edit message: {e}")
            return
        
        # Add number to user's account
        users_data = load_json_data(USERS_FILE, {})
        user_data = users_data.get(user_id, {
            "phone_numbers": [],
            "last_number_time": None,
        })
        
        current_time = time.time()
        
        # Add number to user's list (keep only last 3)
        user_data["phone_numbers"].append(number)
        user_data["phone_numbers"] = user_data["phone_numbers"][-3:]
        user_data["last_number_time"] = current_time
        users_data[user_id] = user_data
        save_json_data(USERS_FILE, users_data)
        
        # Create keyboard with change number and OTP GROUP buttons
        change_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("OTP GROUP", url="https://t.me/alamin15226")],
            [InlineKeyboardButton("🔄 Change Number", callback_data=f"change_number_{country_name}_{platform}")],
            [InlineKeyboardButton("🔙 Back", callback_data='main_menu')]
        ])
        
        # Send new number message
        success_text = (
            "<blockquote><b>✅ Your new number is:</b></blockquote>\n\n"
            f"<blockquote><b>🌍 Country:</b> {flag} {country_name}</blockquote>\n\n"
            f"<blockquote><b>📱 Platform:</b> {platform}</blockquote>\n\n"
            f"<blockquote><b>📞 Number:</b> <code>{number}</code></blockquote>\n\n"
            "<blockquote><b>💡 Tips:</b></blockquote>\n\n"
            f"<blockquote><blockquote>• Use this number to register on {platform}</blockquote>\n\n"
            "<blockquote>• You will be notified automatically when SMS arrives</blockquote></blockquote>"
        )
        
        # Queue the message instead of sending directly
        await MESSAGE_QUEUE.put({
            'chat_id': user_id,
            'text': success_text,
            'parse_mode': ParseMode.HTML,
            'reply_markup': change_keyboard
        })
        
        # Try to edit the message to just a confirmation
        try:
            await query.edit_message_text(
                "<blockquote><b>✅ Your number has been sent successfully. Please check your inbox.</b></blockquote>",
                parse_mode=ParseMode.HTML
            )
        except error.BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                logging.error(f"Failed to edit message: {e}")
        return
    
    elif query.data.startswith('country_'):
        flag = query.data.split('_')[1]
        country_name = COUNTRIES.get(flag, "Unknown")
        
        # Store selected country in context
        context.user_data['selected_country'] = country_name
        context.user_data['selected_flag'] = flag
        
        # Show social platform selection
        if user_id == str(ADMIN_ID):
            # Check if this is for adding or removing numbers
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
            # This path should not be taken by users, but as a fallback:
            social_text = f"<blockquote><b>📱 Which social platform for {flag} {country_name}?</b></blockquote>"
        
        # Initialize selected platforms for admin
        if user_id == str(ADMIN_ID):
            context.user_data['selected_platforms'] = set()
        
        try:
            await query.edit_message_text(
                social_text,
                reply_markup=get_admin_social_keyboard(), # Admin always gets admin keyboard here
                parse_mode=ParseMode.HTML
            )
        except error.BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                logging.error(f"Failed to edit message: {e}")
        return

    elif query.data.startswith('social_'):
        platform = query.data.split('_')[1]
        
        # This logic is now primarily for Admin
        if user_id != str(ADMIN_ID):
            # This should not happen if user flow is correct
            logging.warning(f"User {user_id} reached admin social_ handler.")
            return

        country_name = context.user_data.get('selected_country', 'Unknown')
        flag = context.user_data.get('selected_flag', '🌍')
        
        # Check if this is the "done" button
        if platform == 'done':
            selected_platforms = context.user_data.get('selected_platforms', set())
            
            if not selected_platforms:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="<blockquote><b>❌ Please select at least one platform.</b></blockquote>",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Check if this is for adding or removing numbers
            if context.user_data.get('state') == 'REMOVING_NUMBER':
                # Admin removing numbers
                platforms_text = ", ".join(sorted(selected_platforms))
                removed_count = remove_numbers_for_platforms(country_name, selected_platforms)
                
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"<blockquote><b>✅ Removed {removed_count} numbers for {flag} {country_name} from {platforms_text}!</b></blockquote>",
                    parse_mode=ParseMode.HTML
                )
                
                # Clear state
                context.user_data['state'] = None
                context.user_data.pop('selected_country', None)
                context.user_data.pop('selected_flag', None)
                context.user_data.pop('selected_platforms', None)
            else:
                # Admin adding numbers
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
            # Handle individual platform selection
            selected_platforms = context.user_data.get('selected_platforms', set())
            
            # Toggle platform selection
            if platform in selected_platforms:
                selected_platforms.remove(platform)
            else:
                selected_platforms.add(platform)
            
            context.user_data['selected_platforms'] = selected_platforms
            
            # Update the keyboard to show current selection
            try:
                await query.edit_message_reply_markup(
                    reply_markup=get_admin_social_keyboard(selected_platforms)
                )
            except error.BadRequest as e:
                if "Message is not modified" in str(e):
                    pass
                else:
                    logging.error(f"Failed to edit message: {e}")

    elif query.data.startswith('change_number_'):
        # Handle change number request
        parts = query.data.split('_')
        if len(parts) >= 4:
            country_name = '_'.join(parts[2:-1])  # Handle country names with spaces
            platform = parts[-1]
            
            # Check cooldown
            users_data = load_json_data(USERS_FILE, {})
            user_data = users_data.get(user_id, {})
            
            if 'phone_numbers' not in user_data or not isinstance(user_data['phone_numbers'], list):
                user_data['phone_numbers'] = []

            cooldown = 10
            last_time = user_data.get('last_number_time', 0)
            current_time = time.time()

            if current_time - last_time < cooldown:
                remaining_time = int(cooldown - (current_time - last_time))
                await context.bot.send_message(chat_id=user_id, text=f"<blockquote><b>⚠️ Please wait {remaining_time} seconds.</b></blockquote>", parse_mode=ParseMode.HTML)
                return
            
            # Get new number from file for specific country and platform
            number = await asyncio.to_thread(get_number_from_file_for_platform, country_name, platform)
            if number:
                
                user_data["phone_numbers"].append(number)
                user_data["phone_numbers"] = user_data["phone_numbers"][-3:]
                user_data["last_number_time"] = current_time
                users_data[user_id] = user_data
                save_json_data(USERS_FILE, users_data)
                
                # Get flag for country
                flag = "🌍"
                for f, c in COUNTRIES.items():
                    if c == country_name:
                        flag = f
                        break
                
                # Create keyboard with change number and OTP GROUP buttons
                change_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("OTP GROUP", url="https://t.me/alamin15226")],
                    [InlineKeyboardButton("🔄 Change Number", callback_data=f"change_number_{country_name}_{platform}")],
                    [InlineKeyboardButton("🔙 Back", callback_data='main_menu')]
                ])
                
                # Construct message
                success_text = f"<blockquote><b>✅ Your new number is:</b></blockquote>\n\n" \
                               f"<blockquote><b>🌍 Country:</b> {flag} {country_name}</blockquote>\n\n" \
                               f"<blockquote><b>📱 Platform:</b> {platform}</blockquote>\n\n" \
                               f"<blockquote><b>📞 Number:</b> <code>{number}</code></blockquote>\n\n" \
                               f"<blockquote><b>OTP will be sent to your inbox.</b></blockquote>"
                               
                # Send new number message via queue
                await MESSAGE_QUEUE.put({
                    'chat_id': user_id,
                    'text': success_text,
                    'parse_mode': ParseMode.HTML,
                    'reply_markup': change_keyboard
                })
                
                # Edit original message to show confirmation
                try:
                    await query.edit_message_text(
                        "<blockquote><b>✅ Your number has been sent successfully. Please check your inbox.</b></blockquote>",
                        parse_mode=ParseMode.HTML
                    )
                except error.BadRequest as e:
                    if "Message is not modified" in str(e):
                        pass
                    else:
                        logging.error(f"Failed to edit message: {e}")

            else:
                await context.bot.send_message(chat_id=user_id, text="<blockquote><b>😔 No numbers are available right now. Please try again later.</b></blockquote>", parse_mode=ParseMode.HTML)
                # Send notification to admin
                try:
                    await context.bot.send_message(chat_id=ADMIN_ID, text="<blockquote><b>⚠️ Admin Alert: The bot is out of numbers! Please add new numbers.</b></blockquote>", parse_mode=ParseMode.HTML)
                except Exception as e:
                    logging.error(f"Failed to send out-of-stock notification to admin {ADMIN_ID}: {e}")
    elif query.data == 'account':
        # This button is removed, so this block is now unreachable.
        # We can safely remove it to clean up the code.
        pass

async def handle_add_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle adding numbers when admin sends numbers after selecting country and social."""
    user_id = str(update.effective_user.id)
    
    # Check if admin
    if user_id != str(ADMIN_ID) or context.user_data.get('state') != 'ADDING_NUMBER':
        await start_command(update, context)
        return
    
    # Get selected country and platforms from context
    country = context.user_data.get('selected_country', 'Unknown')
    platforms = context.user_data.get('selected_platforms', set())
    
    if not platforms:
        await update.message.reply_text("<blockquote><b>❌ No platform selected. Please start over with /add.</b></blockquote>", parse_mode=ParseMode.HTML)
        context.user_data['state'] = None
        return
    
    # Get numbers from message
    numbers = update.message.text.split('\n')
    
    added_count = 0
    invalid_count = 0
    response_parts = []
    
    for number_str in numbers:
        number_str = number_str.strip()
        
        # Check for done command
        if number_str.lower() == 'done':
            break
        
        # Validate number
        if 8 <= len(number_str) <= 15 and number_str.isdigit():
            # Add number for all selected platforms
            for platform in platforms:
                await asyncio.to_thread(add_number_to_file, number_str, country, platform)
            
            added_count += 1
            response_parts.append(f"✅ <code>{number_str}</code> added")
        else:
            invalid_count += 1
            response_parts.append(f"❌ <code>{number_str}</code> invalid number")
    
    # Final response
    final_response = (
        f"<blockquote><b>✅ Number adding complete</b></blockquote>\n\n"
        f"<blockquote><b>Total added:</b> {added_count}</blockquote>\n\n"
        f"<blockquote><b>Invalid:</b> {invalid_count}</blockquote>\n\n"
    )
    
    if added_count > 0:
        final_response += "<b>Details:</b>\n\n" + "\n\n".join([f"<blockquote>{part}</blockquote>" for part in response_parts])
    
    # If 'done' command used, reset state
    if any(n.strip().lower() == 'done' for n in numbers):
        final_response += "\n\n<blockquote><b>✅ Number adding finished. Return to /start.</b></blockquote>"
        context.user_data['state'] = None
        context.user_data.pop('selected_country', None)
        context.user_data.pop('selected_flag', None)
        context.user_data.pop('selected_platforms', None)
    else:
        final_response += "\n\n<blockquote><b>...Enter more numbers or type 'done' to finish.</b></blockquote>"
    
    await update.message.reply_text(final_response, parse_mode=ParseMode.HTML)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages - route to appropriate handler based on state."""
    if context.user_data.get('state') == 'ADDING_NUMBER':
        await handle_add_number(update, context)
    else:
        await start_command(update, context)

async def sms_watcher_task(application: Application):
    global manager_instance
    if not manager_instance:
        manager_instance = NewPanelSmsManager()
        
    while not shutdown_event.is_set():
        try:
            # Fetch SMS and save to cache file (This runs synchronously in a thread pool)
            await asyncio.to_thread(manager_instance.scrape_and_save_all_sms)
            
            if not os.path.exists(SMS_CACHE_FILE):
                await asyncio.sleep(2)  # Wait 2 seconds before trying again
                continue

            users_data = load_json_data(USERS_FILE, {})
            sent_sms_keys = load_sent_sms_keys()
            
            phone_to_user_map = {}
            for uid, udata in users_data.items():
                for number in udata.get("phone_numbers", []):
                    phone_to_user_map[number] = uid

            with open(SMS_CACHE_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        sms_data = json.loads(line)
                        phone = sms_data.get('phone')
                        country = sms_data.get('country', 'N/A')
                        provider = sms_data.get('provider', 'N/A')
                        message = sms_data.get('message')
                        otp = extract_otp_from_text(message)
                        
                        # Only proceed if a valid OTP is found
                        if otp == "N/A": continue

                        unique_key = f"{phone}|{otp}"
                        if unique_key in sent_sms_keys:
                            continue

                        # Get number info from our database
                        number_country, number_platform, number_flag = get_number_info(phone)
                        
                        # Use phone number to detect country if database info not available
                        if not number_country:
                            detected_country, detected_flag = detect_country_from_phone(phone)
                            display_country = detected_country
                            display_flag = detected_flag
                        else:
                            display_country = number_country
                            display_flag = number_flag
                        
                        display_platform = number_platform if number_platform else provider

                        owner_id = phone_to_user_map.get(phone)
                        
                        # Check if this number has already received an OTP (for inbox counting)
                        number_otp_key = f"{phone}_otp_received"
                        is_first_otp_for_number = number_otp_key not in sent_sms_keys
                        
                        group_keyboard = InlineKeyboardMarkup([
                            [InlineKeyboardButton("Number Bot", url="https://t.me/otprecevedbot")]
                        ])
                        
                        # Determine service type based on provider or message content
                        is_instagram = "instagram" in provider.lower() or "instagram" in message.lower()
                        is_whatsapp = "whatsapp" in provider.lower() or "whatsapp" in message.lower()
                        
                        if is_instagram:
                            service_icon = "📸"
                            service_name = "Instagram"
                        elif is_whatsapp:
                            service_icon = "📱"
                            service_name = "WhatsApp"
                        else:
                            service_icon = "📱"
                            service_name = provider
                        
                        # Determine the service type for display
                        if is_instagram:
                            service_display = "Instagram"
                            code_label = "Instagram Code"
                        elif is_whatsapp:
                            service_display = "WhatsApp"
                            code_label = "WhatsApp Code"
                        else:
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

                        # Queue message for the group
                        await MESSAGE_QUEUE.put({
                            'chat_id': GROUP_ID, 
                            'text': group_msg, 
                            'parse_mode': ParseMode.HTML, 
                            'reply_markup': group_keyboard
                        })

                        # If SMS belongs to a specific user, also send to their inbox
                        # And only count for the first OTP per number
                        if owner_id and is_first_otp_for_number:
                            # Create the new inline keyboard for the inbox message
                            inbox_keyboard = InlineKeyboardMarkup([
                                [InlineKeyboardButton("OTP GROUP", url="https://t.me/alamin15226")]
                            ])
                            
                            inbox_msg = (
                                f"{service_icon} <b>New {service_display}!</b> ✨\n\n"
                                f"📞 <b>Number:</b> <code>{hide_number(phone)}</code>\n\n"
                                f"🌍 <b>Country:</b> {html_escape(display_country)} {display_flag}\n\n"
                                f"🆔 <b>Service:</b> {html_escape(service_name)}\n\n"
                                f"🔑 <b>{code_label}:</b> <code>{otp}</code>\n\n"
                                f"📝 <b>Full Message:</b>\n\n"
                                f"<blockquote>{html_escape(message)}</blockquote>"
                            )
                            
                            # Queue message for the user
                            await MESSAGE_QUEUE.put({
                                'chat_id': owner_id, 
                                'text': inbox_msg, 
                                'parse_mode': ParseMode.HTML,
                                'reply_markup': inbox_keyboard
                            })
                            
                            # Mark this number as having received an OTP
                            sent_sms_keys.add(number_otp_key)

                        # Mark as sent to prevent duplicate processing
                        sent_sms_keys.add(unique_key)

                    except Exception as e:
                        logging.error(f"Error processing SMS line: {e}")
            
            save_sent_sms_keys(sent_sms_keys)

        except Exception as e:
            logging.error(f"Error in sms_watcher_task: {e}")
        
        await asyncio.sleep(2)  # Changed from 10 seconds to 2 seconds as requested

async def test_group_access(application):
    """Test if bot can send messages to the group"""
    try:
        test_msg = "<blockquote>🤖 Bot is now online and ready to receive SMS!</blockquote>"
        await application.bot.send_message(chat_id=GROUP_ID, text=test_msg, parse_mode=ParseMode.HTML)
        logging.error(f"Group access test successful for group {GROUP_ID}")
    except Exception as e:
        logging.error(f"Group access test FAILED for group {GROUP_ID}: {e}")
        logging.error(f"Please check: 1) Bot is added to group, 2) Bot has 'Send Messages' permission, 3) Group ID is correct")

async def main_bot_loop():
    global manager_instance
    try:
        load_config()
    except Exception as e:
        logging.critical(f"CRITICAL: Could not load config. {e}")
        print(f"CRITICAL: Could not load config. {e}")
        return
        
    manager_instance = NewPanelSmsManager()

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("sms", sms_command)) # User sms command
    application.add_handler(CommandHandler("add", add_command)) # Admin add command
    application.add_handler(CommandHandler("delete", delete_command)) # Admin delete/remove command
    application.add_handler(CommandHandler("remove", delete_command)) # Alias for delete
    application.add_handler(CommandHandler("new", new_session_command)) # Admin update session command
    application.add_handler(CallbackQueryHandler(button_callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    # Test group access
    await test_group_access(application)

    sms_task = asyncio.create_task(sms_watcher_task(application))
    sender_task = asyncio.create_task(rate_limited_sender_task(application)) # NEW SENDER TASK
    
    await shutdown_event.wait()
    
    sms_task.cancel()
    sender_task.cancel() # CANCEL SENDER TASK
    try:
        await sms_task
        await sender_task # WAIT FOR SENDER TASK TO CLEAN UP
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
