"""
Sora Telegram Bot - Python Edition
Based on: sora2-video-generator-v2.js

Features:
1. Telegram bot listens for /generate command
2. Queries NocoDB for "Ready To Generate" records
3. Calls GeminiGen.ai Sora API
4. Webhook endpoint for completion callbacks (or polling fallback)
5. Updates NocoDB + sends Telegram notifications
6. Generates viral captions using Gemini

Deployment: Run on VPS using Docker or systemd service
"""

import os
import json
import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List
import aiohttp
from aiohttp import web
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==================================================
# CONFIGURATION
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

# Webhook server config
WEBHOOK_HOST = os.getenv('WEBHOOK_HOST', '0.0.0.0')
WEBHOOK_PORT = int(os.getenv('WEBHOOK_PORT', '8080'))
WEBHOOK_PATH = '/sora-callback'

# Cambodia Timezone (UTC+7)
CAMBODIA_TZ = timezone(timedelta(hours=7))

# Posting Schedule (Cambodia time UTC+7)
POSTING_TIMES = [8, 21]  # 8:00 AM and 9:00 PM

# Logging
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


# ==================================================
# HELPER FUNCTIONS
# ==================================================

def escape_markdown(text: str) -> str:
    """Escape special characters for Telegram Markdown"""
    if not text:
        return ""
    # Escape markdown special characters
    for char in ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
        text = str(text).replace(char, f'\\{char}')
    return text


def parse_video_length(video_length_value) -> int:
    """Parse video length from Airtable field (e.g., "10s", "15s")"""
    if not video_length_value:
        return 10
    
    # Handle single select object (Baserow returns dict with 'value', Airtable uses 'name')
    if isinstance(video_length_value, dict):
        value_str = video_length_value.get('value') or video_length_value.get('name', '10s')
    else:
        value_str = str(video_length_value)
    
    # Extract number from string
    match = re.search(r'(\d+)', value_str)
    if match:
        duration = int(match.group(1))
        if duration in [10, 15]:
            return duration
    return 10


def get_next_available_slot(scheduled_slots: List[datetime], page_id: str) -> datetime:
    """Find next available posting slot in Cambodia time (UTC+7)"""
    # Get current time in Cambodia timezone
    now_utc = datetime.now(timezone.utc)
    now_cambodia = now_utc.astimezone(CAMBODIA_TZ)
    
    logger.info(f"DEBUG: Current Cambodia time: {now_cambodia.strftime('%Y-%m-%d %H:%M:%S')}")
    
    for day_offset in range(30):
        # Calculate current day in Cambodia time
        current_day = now_cambodia + timedelta(days=day_offset)
        current_day = current_day.replace(hour=0, minute=0, second=0, microsecond=0)
        
        for hour in POSTING_TIMES:
            slot_time = current_day.replace(hour=hour)
            
            # Skip past slots
            if slot_time <= now_cambodia:
                logger.info(f"DEBUG: Skipping past slot: {slot_time.strftime('%Y-%m-%d %H:%M')}")
                continue
            
            # Check if slot is taken (compare without timezone for simplicity)
            slot_naive = slot_time.replace(tzinfo=None)
            is_taken = any(
                scheduled.replace(second=0, microsecond=0, tzinfo=None) == slot_naive.replace(second=0, microsecond=0)
                for scheduled in scheduled_slots
            )
            
            if not is_taken:
                logger.info(f"DEBUG: Found available slot: {slot_time.strftime('%Y-%m-%d %H:%M')} Cambodia time")
                # Keep timezone info so Airtable interprets correctly as UTC+7
                return slot_time
    
    # Fallback: tomorrow first slot
    fallback = now_cambodia + timedelta(days=1)
    return fallback.replace(hour=POSTING_TIMES[0], minute=0, second=0, microsecond=0)


# ==================================================
# TELEGRAM FUNCTIONS
# ==================================================

async def send_telegram_message(text: str, chat_ids: List[str] = None):
    """Send message to Telegram"""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("No Telegram bot token configured")
        return
    
    chat_ids = chat_ids or TELEGRAM_CHAT_IDS
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    async with aiohttp.ClientSession() as session:
        for chat_id in chat_ids:
            try:
                payload = {
                    "chat_id": chat_id.strip(),
                    "text": text,
                    "parse_mode": "Markdown"
                }
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        logger.error(f"Telegram error: {await resp.text()}")
            except Exception as e:
                logger.error(f"Failed to send Telegram message: {e}")


async def send_telegram_with_keyboard(text: str, chat_id: str, keyboard: list = None):
    """Send message with reply keyboard buttons"""
    if not TELEGRAM_BOT_TOKEN:
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    
    if keyboard:
        payload["reply_markup"] = {
            "keyboard": keyboard,
            "resize_keyboard": True,
            "persistent": True
        }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.error(f"Telegram error: {await resp.text()}")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")


# Default keyboard buttons
DEFAULT_KEYBOARD = [
    ["🎬 Generate", "📊 Status"],
    ["❓ Help"]
]


async def poll_telegram_updates():
    """Long-poll for Telegram updates (simple bot implementation)"""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("No Telegram bot token - skipping Telegram polling")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    offset = 0
    
    logger.info("📱 Starting Telegram bot polling...")
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                params = {"offset": offset, "timeout": 30}
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(5)
                        continue
                    
                    data = await resp.json()
                    
                    for update in data.get("result", []):
                        offset = update["update_id"] + 1
                        
                        message = update.get("message", {})
                        text = message.get("text", "")
                        chat_id = str(message.get("chat", {}).get("id", ""))
                        
                        # Handle both commands and button text
                        if text.startswith("/generate") or text == "🎬 Generate":
                            logger.info(f"Received generate command from {chat_id}")
                            asyncio.create_task(handle_generate_command(chat_id))
                        elif text.startswith("/status") or text == "📊 Status":
                            asyncio.create_task(handle_status_command(chat_id))
                        elif text.startswith("/start"):
                            # Show welcome message with keyboard
                            await send_telegram_with_keyboard(
                                "🎬 *Sora Video Generator Bot*\n\n"
                                "Use the buttons below or type commands:\n"
                                "• /generate - Start video generation\n"
                                "• /status - Check pending jobs\n"
                                "• /help - Show help",
                                chat_id,
                                DEFAULT_KEYBOARD
                            )
                        elif text.startswith("/help") or text == "❓ Help":
                            await send_telegram_with_keyboard(
                                "🎬 *Sora Video Generator Bot*\n\n"
                                "Commands:\n"
                                "/generate - Start video generation for ready records\n"
                                "/status - Check pending jobs\n"
                                "/help - Show this message\n\n"
                                "📋 *Generation Conditions:*\n"
                                "• Ready To Generate = ✓\n"
                                "• Status = Draft\n"
                                "• Prompt filled\n"
                                "• Target Page set",
                                chat_id,
                                DEFAULT_KEYBOARD
                            )
            except Exception as e:
                logger.error(f"Telegram polling error: {e}")
                await asyncio.sleep(5)


async def handle_status_command(chat_id: str):
    """Handle /status command"""
    if not pending_jobs:
        await send_telegram_message("📭 No pending video generations.", [chat_id])
        return
    
    status_lines = ["📊 *Pending Jobs:*\n"]
    for uuid, job in pending_jobs.items():
        status_lines.append(f"• `{uuid[:8]}...` - {job.get('status', 'unknown')}")
    
    await send_telegram_message("\n".join(status_lines), [chat_id])


# ==================================================
# SORA API FUNCTIONS
# ==================================================

async def generate_video(prompt: str, duration: int = 10) -> str:
    """Call GeminiGen.ai Sora 2 API to generate video"""
    
    # Validate duration
    if duration not in [10, 15]:
        duration = 10
    
    logger.info(f"🎬 Calling Sora API (sora-2, {duration}s)...")
    
    url = "https://api.geminigen.ai/uapi/v1/video-gen/sora"
    
    # Create form data
    form_data = aiohttp.FormData()
    form_data.add_field('prompt', prompt)
    form_data.add_field('model', 'sora-2')
    form_data.add_field('resolution', 'small')
    form_data.add_field('duration', str(duration))
    form_data.add_field('aspect_ratio', 'portrait')
    
    headers = {'x-api-key': SORA_API_KEY}
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=form_data, headers=headers) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                raise Exception(f"Sora API error: {resp.status} - {error_text}")
            
            data = await resp.json()
            uuid = data.get('uuid')
            
            if not uuid:
                raise Exception("No UUID in response")
            
            logger.info(f"✅ Generation started: {uuid}")
            return uuid


async def poll_for_completion(uuid: str, max_attempts: int = 30, delay_seconds: int = 15) -> str:
    """Poll for video completion (fallback if webhook not working)"""
    
    logger.info(f"⏳ Polling for completion: {uuid}")
    
    url = f"https://api.geminigen.ai/uapi/v1/history/{uuid}"
    headers = {'x-api-key': SORA_API_KEY}
    
    async with aiohttp.ClientSession() as session:
        for attempt in range(1, max_attempts + 1):
            await asyncio.sleep(delay_seconds)
            
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        logger.warning(f"  Poll attempt {attempt}: Status {resp.status}")
                        continue
                    
                    data = await resp.json()
                    status = data.get('status')  # 1=processing, 2=completed, 3=failed
                    percentage = data.get('status_percentage', 0)
                    
                    logger.info(f"  Attempt {attempt}/{max_attempts}: {data.get('status_desc', 'Processing')} ({percentage}%)")
                    
                    if status == 2:  # Completed
                        # Try both response formats
                        if data.get('media_url'):
                            return data['media_url']
                        
                        generated = data.get('generated_video', [])
                        if generated:
                            return generated[0].get('video_url') or generated[0].get('file_download_url')
                        
                        raise Exception("No video URL in response")
                    
                    elif status == 3:  # Failed
                        raise Exception(f"Generation failed: {data.get('error_message', 'Unknown')}")
                        
            except aiohttp.ClientError as e:
                logger.warning(f"  Poll attempt {attempt}: {e}")
    
    raise Exception(f"Timed out after {max_attempts * delay_seconds / 60} minutes")


# ==================================================
# GEMINI CAPTION GENERATION
# ==================================================

async def generate_caption(video_prompt: str) -> str:
    """Generate viral caption using Gemini"""
    
    logger.info("📝 Generating caption with Gemini...")
    
    model = "gemini-3-flash-preview"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
    
    instruction = f"""You are a viral social media copywriter for TikTok/Reels.

VIDEO DESCRIPTION:
"{video_prompt}"

RULES:
1. Do NOT describe the video - viewers can see it
2. Ignore technical terms (4k, cinematic, lighting, etc.)
3. Create a caption that adds humor, context, or relatability
4. VARY your style - do NOT always use POV format
5. Keep the caption SHORT (under 100 characters)

CAPTION STYLES (rotate between these):
• Observation: "The way he just accepted defeat"
• Relatable: "Why is this actually me though"
• Commentary: "No one is talking about how..."
• Question: "Tag someone who does this"
• POV: "POV: You explain things like this"
• Main Character: "Main character energy"

Generate a viral caption and exactly 3 hashtags. Return as JSON: {{"caption": "your caption here", "hashtags": ["#Tag1", "#Tag2", "#Tag3"]}}"""
    
    payload = {
        "contents": [{"parts": [{"text": instruction}]}],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 256
        }
    }
    
    async with aiohttp.ClientSession() as session:
        for attempt in range(3):
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.warning(f"Gemini API error {resp.status}: {error_text[:200]}")
                        continue
                    
                    data = await resp.json()
                    raw_text = data['candidates'][0]['content']['parts'][0]['text'].strip()
                    
                    logger.info(f"DEBUG: Gemini raw response: {raw_text[:300]}")
                    
                    # Try to parse JSON (handle various formats)
                    caption = None
                    hashtags = ['#Viral', '#Trending', '#ForYou']
                    
                    try:
                        # Clean the response - remove markdown code blocks if present
                        clean_text = raw_text
                        if '```json' in clean_text:
                            clean_text = clean_text.split('```json')[1].split('```')[0]
                        elif '```' in clean_text:
                            clean_text = clean_text.split('```')[1].split('```')[0]
                        
                        # Remove newlines inside strings that break JSON
                        clean_text = clean_text.strip()
                        
                        caption_data = json.loads(clean_text)
                        caption = caption_data.get('caption', '').strip()
                        hashtags = caption_data.get('hashtags', hashtags)
                        
                    except json.JSONDecodeError:
                        # Fallback: Extract caption using regex
                        logger.warning("JSON parse failed, trying regex extraction")
                        
                        # Try to find "caption": "..." pattern
                        caption_match = re.search(r'"caption"\s*:\s*"([^"]+)"', raw_text)
                        if caption_match:
                            caption = caption_match.group(1)
                        
                        # Try to find hashtags
                        hashtag_matches = re.findall(r'#\w+', raw_text)
                        if hashtag_matches:
                            hashtags = hashtag_matches[:3]
                    
                    if caption and len(caption) >= 5:
                        # Ensure hashtags start with #
                        formatted_tags = [tag if tag.startswith('#') else f'#{tag}' for tag in hashtags[:3]]
                        result = f"{caption}\n{' '.join(formatted_tags)}"
                        logger.info(f"✅ Generated caption: {result[:100]}...")
                        return result
                    
            except Exception as e:
                logger.warning(f"Caption attempt {attempt + 1} failed: {e}")
    
    # Fallback
    logger.warning("All caption attempts failed, using fallback")
    return "Check this out! 🔥\n#Viral #Trending #ForYou"


# ==================================================
# BASEROW API FUNCTIONS
# ==================================================

# Global token cache
_baserow_token = None
_token_expiry = None

async def get_baserow_token() -> str:
    """Get Baserow JWT token (with caching)"""
    global _baserow_token, _token_expiry
    
    # Return cached token if valid
    if _baserow_token and _token_expiry and datetime.now(timezone.utc) < _token_expiry:
        return _baserow_token
    
    # Login to get new token
    url = f"{BASEROW_URL}/api/user/token-auth/"
    payload = {'email': BASEROW_USERNAME, 'password': BASEROW_PASSWORD}
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                raise Exception(f"Baserow login failed: {error_text}")
            
            data = await resp.json()
            _baserow_token = data.get('access_token') or data.get('token')
            # Token expires in 60 min, cache for 55 min
            _token_expiry = datetime.now(timezone.utc) + timedelta(minutes=55)
            return _baserow_token


async def get_baserow_headers() -> dict:
    """Get Baserow API headers with JWT token"""
    token = await get_baserow_token()
    return {
        'Authorization': f'JWT {token}',
        'Content-Type': 'application/json'
    }


async def get_page_name(page_id) -> str:
    """Lookup page name from Facebook Pages table"""
    try:
        url = f"{BASEROW_URL}/api/database/rows/table/{FACEBOOK_PAGES_TABLE}/{page_id}/"
        headers = await get_baserow_headers()
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params={'user_field_names': 'true'}) as resp:
                if resp.status != 200:
                    logger.warning(f"Failed to get page name: {resp.status}")
                    return 'Unknown'
                
                data = await resp.json()
                return data.get('Page Name', 'Unknown')
    except Exception as e:
        logger.warning(f"Could not get page name for {page_id}: {e}")
        return 'Unknown'



async def upload_video_to_baserow(video_url: str, record_id) -> str:
    """Download video from URL and upload to Baserow file storage"""
    try:
        logger.info(f"📥 Downloading video for upload to Baserow...")
        
        # Download video from R2/external URL
        async with aiohttp.ClientSession() as session:
            async with session.get(video_url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to download video: {resp.status}")
                    return None
                video_data = await resp.read()
        
        logger.info(f"📤 Uploading video to Baserow ({len(video_data) / 1024 / 1024:.1f} MB)...")
        
        # Upload to Baserow
        upload_url = f"{BASEROW_URL}/api/user-files/upload-file/"
        token = await get_baserow_token()
        headers = {'Authorization': f'JWT {token}'}
        
        form = aiohttp.FormData()
        form.add_field('file', video_data, 
                       filename=f'video_{record_id}.mp4', 
                       content_type='video/mp4')
        
        async with aiohttp.ClientSession() as session:
            async with session.post(upload_url, headers=headers, data=form) as resp:
                if resp.status not in [200, 201]:
                    error_text = await resp.text()
                    logger.error(f"Failed to upload video: {resp.status} - {error_text}")
                    return None
                
                data = await resp.json()
                file_name = data.get('name')
                logger.info(f"✅ Video uploaded to Baserow: {file_name}")
                return file_name
                
    except Exception as e:
        logger.error(f"Error uploading video to Baserow: {e}")
        return None


async def get_ready_records():
    """Get records marked as Ready To Generate. Returns (records, stats)"""
    # Baserow API - fetch all and filter in Python
    url = f"{BASEROW_URL}/api/database/rows/table/{REELS_GENERATION_TABLE}/"
    
    params = {
        'user_field_names': 'true',
        'size': 200
    }
    
    # Stats for better error messages
    stats = {
        'total': 0,
        'not_ready': 0,
        'not_draft': 0,
        'no_prompt': 0,
        'no_target': 0,
        'ready': 0
    }
    
    try:
        headers = await get_baserow_headers()
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    logger.error(f"Failed to get records: {resp.status} - {error}")
                    return [], stats
                
                data = await resp.json()
                records = data.get('results', [])
                stats['total'] = len(records)
                
                logger.info(f"DEBUG: Fetched {len(records)} total records from Baserow")
                
                ready_records = []
                for record in records:
                    # Check Ready To Generate (boolean)
                    ready_to_gen = record.get('Ready To Generate', False)
                    if isinstance(ready_to_gen, dict):
                        ready_to_gen = ready_to_gen.get('value', False)
                    
                    # Check Status
                    status_field = record.get('Status', {})
                    if isinstance(status_field, dict):
                        status = status_field.get('value', '')
                    else:
                        status = str(status_field) if status_field else ''
                    
                    has_prompt = bool(record.get('Prompt'))
                    target_page = record.get('Target Page', [])
                    has_target = bool(target_page and len(target_page) > 0)
                    
                    # Track why records are filtered out
                    if not ready_to_gen:
                        stats['not_ready'] += 1
                    if status != 'Draft':
                        stats['not_draft'] += 1
                    if not has_prompt:
                        stats['no_prompt'] += 1
                    if not has_target:
                        stats['no_target'] += 1
                    
                    if ready_to_gen and status == 'Draft' and has_prompt and has_target:
                        ready_records.append(record)
                
                stats['ready'] = len(ready_records)
                logger.info(f"DEBUG: Stats = {stats}")
                return ready_records, stats
    
    except Exception as e:
        logger.error(f"Error fetching ready records: {e}")
        import traceback
        traceback.print_exc()
        return [], stats



async def update_record_status(record_id, status: str, video_url: str = None):
    """Update Baserow record status with video upload and retry on token expiry"""
    global _baserow_token, _token_expiry
    
    url = f"{BASEROW_URL}/api/database/rows/table/{REELS_GENERATION_TABLE}/{record_id}/"
    
    # Use single_select option ID for Status field
    status_id = REELS_STATUS_OPTIONS.get(status)
    if status_id:
        update_fields = {'Status': status_id}
    else:
        logger.warning(f"Unknown status '{status}', using string fallback")
        update_fields = {'Status': status}
    
    if video_url:
        # Upload video to Baserow and get file token
        file_token = await upload_video_to_baserow(video_url, record_id)
        if file_token:
            update_fields['Video'] = [{'name': file_token}]
        update_fields['Ready To Generate'] = False
    
    for attempt in range(2):  # Retry once on 401
        try:
            headers = await get_baserow_headers()
            async with aiohttp.ClientSession() as session:
                async with session.patch(url, headers=headers, json=update_fields, params={'user_field_names': 'true'}) as resp:
                    if resp.status in [200, 201]:
                        logger.info(f"✅ Updated record {record_id}: Status={status}")
                        return True
                    elif resp.status == 401 and attempt == 0:
                        logger.warning("Token expired, refreshing...")
                        _baserow_token = None
                        _token_expiry = None
                        continue
                    else:
                        error_text = await resp.text()
                        logger.error(f"Failed to update record: {resp.status} - {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Error updating record {record_id}: {e}")
            return False
    
    return False



async def create_post_queue_record(source_record_id, page_id, video_url: str, caption: str):
    """Create Post Queue record with scheduling"""
    
    # Get existing scheduled posts
    list_url = f"{BASEROW_URL}/api/database/rows/table/{POST_QUEUE_TABLE}/"
    params = {
        'user_field_names': 'true',
        'filter__Status__single_select_equal': 'Scheduled',
        'size': 100
    }
    
    scheduled_slots = []
    
    try:
        headers = await get_baserow_headers()
        async with aiohttp.ClientSession() as session:
            async with session.get(list_url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    posts = data.get('results', [])
                    
                    for post in posts:
                        schedule_for = post.get('Schedule For')
                        if schedule_for:
                            try:
                                scheduled_slots.append(datetime.fromisoformat(schedule_for.replace('Z', '+00:00')))
                            except:
                                pass
    except Exception as e:
        logger.warning(f"Could not fetch scheduled slots: {e}")
    
    # Find next slot
    schedule_time = get_next_available_slot(scheduled_slots, str(page_id))
    
    # Create record
    logger.info(f"Creating Post Queue record: Source={source_record_id}, Page={page_id}")
    
    create_url = f"{BASEROW_URL}/api/database/rows/table/{POST_QUEUE_TABLE}/"
    
    # Baserow field formats:
    # - Source Reel: text field (store as string)
    # - Page Name: link_row field (use ID array)
    # - Status: single_select field (use option ID)
    create_payload = {
        'Source Reel': str(source_record_id) if source_record_id else '',
        'Page Name': [int(page_id)] if page_id else [],
        'Video URL': video_url,
        'Schedule For': schedule_time.isoformat(),
        'Status': POST_QUEUE_STATUS_OPTIONS.get('Scheduled', 3060),
        'Caption': caption
    }
    
    logger.info(f"DEBUG: Creating Post Queue record with payload: {json.dumps(create_payload, default=str)}")
    
    for attempt in range(2):  # Retry once on 401
        try:
            headers = await get_baserow_headers()
            async with aiohttp.ClientSession() as session:
                async with session.post(create_url, headers=headers, json=create_payload, params={'user_field_names': 'true'}) as resp:
                    if resp.status in [200, 201]:
                        new_record = await resp.json()
                        logger.info(f"✅ Created Post Queue record ID: {new_record.get('id')}")
                        return new_record, schedule_time
                    elif resp.status == 401 and attempt == 0:
                        logger.warning("Token expired, refreshing...")
                        global _baserow_token, _token_expiry
                        _baserow_token = None
                        _token_expiry = None
                        continue
                    else:
                        error_text = await resp.text()
                        logger.error(f"Failed to create post queue record: {resp.status} - {error_text}")
                        return None, schedule_time
        except Exception as e:
            logger.error(f"Error creating post queue record: {e}")
            return None, schedule_time
    
    return None, schedule_time


# ==================================================
# MAIN GENERATION HANDLER
# ==================================================

async def handle_generate_command(chat_id: str = None):
    """Handle /generate command - process ready records"""
    
    await send_telegram_message("🔍 Looking for records to generate...", [chat_id] if chat_id else None)
    
    try:
        # Get ready records with stats
        ready_records, stats = await get_ready_records()
        
        if not ready_records:
            # Build detailed message about why no records match
            msg_parts = ["📊 *No records ready for generation*\n"]
            
            if stats['total'] == 0:
                msg_parts.append("❌ No records found in table")
            else:
                msg_parts.append(f"📋 Total records: {stats['total']}\n")
                msg_parts.append("*Reasons for exclusion:*")
                
                if stats['not_ready'] > 0:
                    msg_parts.append(f"• ☐ Ready To Generate unchecked: {stats['not_ready']}")
                if stats['not_draft'] > 0:
                    msg_parts.append(f"• ⏸️ Status not 'Draft': {stats['not_draft']}")
                if stats['no_prompt'] > 0:
                    msg_parts.append(f"• 📝 Missing Prompt: {stats['no_prompt']}")
                if stats['no_target'] > 0:
                    msg_parts.append(f"• 🎯 Missing Target Page: {stats['no_target']}")
                
                msg_parts.append("\n💡 *To generate:*")
                msg_parts.append("✓ Check 'Ready To Generate'")
                msg_parts.append("✓ Set Status to 'Draft'")
                msg_parts.append("✓ Fill in Prompt")
                msg_parts.append("✓ Select Target Page")
            
            await send_telegram_message("\n".join(msg_parts), [chat_id] if chat_id else None)
            return
        
        await send_telegram_message(
            f"📋 Found {len(ready_records)} record(s) to process\n"
            "🚀 Starting video generation...",
            [chat_id] if chat_id else None
        )
        
        # Process each record
        for record in ready_records:
            record_id = record.get('id')  # Baserow uses lowercase 'id'
            
            prompt = record.get('Prompt', '')
            target_page = record.get('Target Page', [])
            video_length = record.get('Video Length')
            
            if not target_page:
                logger.warning(f"Record {record_id}: No target page")
                continue
            
            # Baserow returns link_row as [{id: X, value: "name"}]
            if isinstance(target_page, list) and len(target_page) > 0:
                page_id = target_page[0].get('id') if isinstance(target_page[0], dict) else target_page[0]
                page_name = target_page[0].get('value', 'Unknown') if isinstance(target_page[0], dict) else 'Unknown'
            else:
                page_id = target_page
                page_name = await get_page_name(page_id)
            
            logger.info(f"DEBUG: Extracted page_id = {page_id}, page_name = {page_name}")
            logger.info(f"DEBUG: Video Length raw = {video_length}, parsed duration = {parse_video_length(video_length)}s")
            
            duration = parse_video_length(video_length)
            
            try:
                # Update status to Processing (NocoDB uses "Processing" not "Progressing")
                await update_record_status(record_id, 'Processing')
                
                # Call Sora API
                uuid = await generate_video(prompt, duration)
                
                # Add to pending jobs
                pending_jobs[uuid] = {
                    'record_id': record_id,
                    'prompt': prompt,
                    'page_id': page_id,
                    'page_name': page_name,
                    'status': 'generating',
                    'chat_id': chat_id
                }
                
                await send_telegram_message(
                    f"🎬 Started: `{uuid[:12]}...`\n"
                    f"📄 Prompt: {prompt[:50]}...\n"
                    f"⏳ Waiting for webhook callback (3-5 min)",
                    [chat_id] if chat_id else None
                )
                
                # Webhook will handle completion - no polling needed
                logger.info(f"Job {uuid} registered, waiting for webhook callback")
                
            except Exception as e:
                logger.error(f"Error processing {record_id}: {e}")
                await update_record_status(record_id, 'Error')
                await send_telegram_message(f"❌ Error: {escape_markdown(str(e))}", [chat_id] if chat_id else None)
    
    except Exception as e:
        logger.error(f"Generate command error: {e}")
        await send_telegram_message(f"❌ Error: {escape_markdown(str(e))}", [chat_id] if chat_id else None)


async def poll_and_complete(uuid: str):
    """Poll for completion and process result (fallback)"""
    
    job = pending_jobs.get(uuid)
    if not job:
        return
    
    try:
        # Poll for completion
        video_url = await poll_for_completion(uuid)
        
        # Complete the job
        await complete_video_generation(uuid, video_url)
        
    except Exception as e:
        logger.error(f"Polling failed for {uuid}: {e}")
        job['status'] = 'failed'
        job['error'] = str(e)
        
        await update_record_status(job['record_id'], 'Error')
        await send_telegram_message(
            f"❌ Video generation failed\n"
            f"UUID: `{uuid[:12]}...`\n"
            f"Error: {escape_markdown(str(e))}",
            [job.get('chat_id')] if job.get('chat_id') else None
        )
        
        del pending_jobs[uuid]


async def complete_video_generation(uuid: str, video_url: str):
    """Complete video generation - save to Airtable, generate caption, notify"""
    
    job = pending_jobs.get(uuid)
    if not job:
        logger.warning(f"No pending job found for {uuid}")
        return
    
    logger.info(f"✅ Completing generation: {uuid}")
    
    record_id = job['record_id']
    prompt = job['prompt']
    page_id = job['page_id']
    page_name = job['page_name']
    chat_id = job.get('chat_id')
    
    try:
        # Update Airtable with video
        await update_record_status(record_id, 'Completed', video_url=video_url)
        
        # Generate caption
        caption = await generate_caption(prompt)
        
        # Create Post Queue record
        post_record, schedule_time = await create_post_queue_record(
            record_id, page_id, video_url, caption
        )
        
        schedule_str = schedule_time.strftime("%b %d at %I:%M %p")
        
        # Notify Telegram
        await send_telegram_message(
            f"✅ *Video Generation Complete!*\n\n"
            f"📎 [Video URL]({video_url})\n\n"
            f"📝 Caption:\n{caption}\n\n"
            f"📅 Scheduled for: {schedule_str}\n"
            f"📄 Page: {page_name}",
            [chat_id] if chat_id else None
        )
        
        job['status'] = 'completed'
        
    except Exception as e:
        logger.error(f"Error completing {uuid}: {e}")
        await send_telegram_message(
            f"⚠️ Video ready but save failed\n"
            f"📎 URL: {video_url}\n"
            f"Error: {escape_markdown(str(e))}",
            [chat_id] if chat_id else None
        )
    
    finally:
        del pending_jobs[uuid]


# ==================================================
# WEBHOOK HANDLER
# ==================================================

async def handle_sora_webhook(request):
    """Handle GeminiGen.ai webhook callback"""
    
    try:
        data = await request.json()
        
        # DEBUG: Log full payload to understand structure
        logger.info(f"📥 Webhook raw payload: {json.dumps(data, default=str)[:500]}")
        
        # GeminiGen uses "event_name" not "event" (confirmed from actual payload)
        # Format: {event_name: "VIDEO_GENERATION_COMPLETED", event_uuid: "...", data: {uuid: "...", media_url: "..."}}
        
        event = data.get('event_name') or data.get('event')
        payload = data.get('data', {})
        uuid = payload.get('uuid')
        
        logger.info(f"📥 Webhook received: {event} for {uuid}")
        
        if event == 'VIDEO_GENERATION_COMPLETED':
            video_url = payload.get('media_url')
            if uuid and video_url:
                await complete_video_generation(uuid, video_url)
            else:
                logger.warning(f"Missing video_url in completed webhook for {uuid}. Payload: {json.dumps(payload, default=str)[:200]}")
        
        elif event == 'VIDEO_GENERATION_FAILED':
            error_msg = payload.get('error_message', 'Unknown error')
            
            job = pending_jobs.get(uuid)
            if job:
                await update_record_status(job['record_id'], 'Error')
                await send_telegram_message(
                    f"❌ Video generation failed\n"
                    f"UUID: `{uuid[:12]}...`\n"
                    f"Error: {error_msg}",
                    [job.get('chat_id')] if job.get('chat_id') else None
                )
                del pending_jobs[uuid]
        else:
            logger.info(f"📥 Ignoring webhook event: {event}")
        
        return web.Response(text="OK", status=200)
    
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        import traceback
        traceback.print_exc()
        return web.Response(text="Error", status=500)


async def health_check(request):
    """Health check endpoint"""
    return web.json_response({
        "status": "ok",
        "pending_jobs": len(pending_jobs),
        "timestamp": datetime.now().isoformat()
    })


# ==================================================
# MAIN
# ==================================================

async def main():
    """Main entry point"""
    
    logger.info("=" * 60)
    logger.info("🎬 Sora Telegram Bot - Starting...")
    logger.info("=" * 60)
    
    # Validate required environment variables
    missing_vars = []
    
    if not TELEGRAM_BOT_TOKEN:
        missing_vars.append("TELEGRAM_BOT_TOKEN")
    if not SORA_API_KEY:
        missing_vars.append("SORA_API_KEY")
    if not GEMINI_API_KEY:
        missing_vars.append("GEMINI_API_KEY")
    if not BASEROW_URL:
        missing_vars.append("BASEROW_URL")
    if not BASEROW_USERNAME:
        missing_vars.append("BASEROW_USERNAME")
    if not BASEROW_PASSWORD:
        missing_vars.append("BASEROW_PASSWORD")
    if not TELEGRAM_CHAT_IDS or TELEGRAM_CHAT_IDS == ['']:
        missing_vars.append("TELEGRAM_CHAT_IDS")
    
    if missing_vars:
        logger.error("❌ Missing required environment variables:")
        for var in missing_vars:
            logger.error(f"   - {var}")
        logger.error("")
        logger.error("Please create a .env file with all required variables.")
        logger.error("See .env.example for reference.")
        return
    
    # Create web app for webhook
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_sora_webhook)
    app.router.add_get('/health', health_check)
    
    # Start webhook server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEBHOOK_HOST, WEBHOOK_PORT)
    await site.start()
    
    logger.info(f"🌐 Webhook server started on http://{WEBHOOK_HOST}:{WEBHOOK_PORT}")
    logger.info(f"📍 Webhook endpoint: {WEBHOOK_PATH}")
    logger.info(f"💡 Configure GeminiGen webhook URL: http://YOUR_VPS_IP:{WEBHOOK_PORT}{WEBHOOK_PATH}")
    
    # Start Telegram polling
    await poll_telegram_updates()


if __name__ == "__main__":
    asyncio.run(main())
