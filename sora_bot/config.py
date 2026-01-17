"""
Sora Bot Configuration Module

Contains all configuration constants, API keys, and global state.
"""

import os
import logging
from datetime import timedelta, timezone
from typing import Dict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==================================================
# API CONFIGURATION
# ==================================================

# API Keys (from .env - NO DEFAULTS, must be configured)
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
SORA_API_KEY = os.getenv('SORA_API_KEY')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

# Baserow Configuration
BASEROW_URL = os.getenv('BASEROW_URL', 'https://baserow.nextlabz.site')
BASEROW_USERNAME = os.getenv('BASEROW_USERNAME')  # email
BASEROW_PASSWORD = os.getenv('BASEROW_PASSWORD')

# Telegram Chat IDs to notify (comma-separated in env)
TELEGRAM_CHAT_IDS = os.getenv('TELEGRAM_CHAT_IDS', '').split(',')

# ==================================================
# DATABASE CONFIGURATION
# ==================================================

# Baserow Table IDs
REELS_GENERATION_TABLE = 748
POST_QUEUE_TABLE = 749
FACEBOOK_PAGES_TABLE = 747

# Baserow Status field option IDs (single_select type)
# Reels Generation table (748)
REELS_STATUS_OPTIONS = {
    'Draft': 3054,
    'Processing': 3055,
    'Completed': 3056,
    'Error': 3057
}

# Post Queue Reels table (749)
POST_QUEUE_STATUS_OPTIONS = {
    'Scheduled': 3060,
    'Posted': 3061,
    'Error': 3062
}

# ==================================================
# SERVER CONFIGURATION
# ==================================================

# Webhook server config
WEBHOOK_HOST = os.getenv('WEBHOOK_HOST', '0.0.0.0')
WEBHOOK_PORT = int(os.getenv('WEBHOOK_PORT', '8080'))
WEBHOOK_PATH = '/sora-callback'

# ==================================================
# TIMEZONE & SCHEDULING
# ==================================================

# Cambodia Timezone (UTC+7)
CAMBODIA_TZ = timezone(timedelta(hours=7))

# Posting Schedule (Cambodia time UTC+7)
POSTING_TIMES = [8, 21]  # 8:00 AM and 9:00 PM

# ==================================================
# LOGGING
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================================================
# GLOBAL STATE
# ==================================================

# Track pending generations (uuid -> job_info)
pending_jobs: Dict[str, dict] = {}

# Track processed UUIDs to prevent duplicate webhook processing
processed_uuids: set = set()

# Baserow token cache
baserow_token = None
token_expiry = None
