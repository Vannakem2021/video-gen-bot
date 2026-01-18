"""
Sora Bot Telegram Client

Functions for interacting with the Telegram Bot API.
"""

import aiohttp
from typing import List, Optional

from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS, logger, pending_jobs

# HTTP Timeout
DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=30)

# Default keyboard buttons
DEFAULT_KEYBOARD = [
    ["🎬 Generate", "📊 Status"],
    ["❓ Help"]
]


async def send_telegram_message(text: str, chat_ids: List[str] = None):
    """Send message to Telegram"""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN not configured")
        return
    
    chat_ids = chat_ids or TELEGRAM_CHAT_IDS
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    async with aiohttp.ClientSession(timeout=DEFAULT_TIMEOUT) as session:
        for chat_id in chat_ids:
            if not chat_id:
                continue
            try:
                payload = {
                    'chat_id': chat_id,
                    'text': text,
                    'parse_mode': 'Markdown'
                }
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        error = await resp.text()
                        logger.error(f"Failed to send Telegram message: {error}")
            except Exception as e:
                logger.error(f"Telegram error: {e}")


async def send_telegram_with_keyboard(text: str, chat_id: str, keyboard: list = None):
    """Send message with reply keyboard buttons"""
    if not TELEGRAM_BOT_TOKEN:
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    keyboard = keyboard or DEFAULT_KEYBOARD
    
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'Markdown',
        'reply_markup': {
            'keyboard': keyboard,
            'resize_keyboard': True,
            'one_time_keyboard': False
        }
    }
    
    async with aiohttp.ClientSession(timeout=DEFAULT_TIMEOUT) as session:
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    logger.error(f"Failed to send keyboard message: {error}")
        except Exception as e:
            logger.error(f"Telegram keyboard error: {e}")


async def handle_status_command(chat_id: str):
    """Handle /status command"""
    pending_count = len(pending_jobs)
    status_text = (
        f"📊 *Bot Status*\n\n"
        f"Pending jobs: {pending_count}\n"
        f"Bot is running normally ✅"
    )
    await send_telegram_with_keyboard(status_text, chat_id)



