"""
Sora Bot Helper Functions

Utility functions for text processing, scheduling, and data parsing.
"""

import re
from datetime import datetime, timedelta
from typing import List

from .config import CAMBODIA_TZ, POSTING_TIMES, logger


def escape_markdown(text: str) -> str:
    """Escape special characters for Telegram Markdown"""
    if not text:
        return ''
    # Escape these characters: _ * [ ] ( ) ~ ` > # + - = | { } . !
    special_chars = r'\_*[]()~`>#+-=|{}.!'
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text


def parse_video_length(video_length_value) -> str:
    """Parse video length from Baserow field (e.g., "10s", "15s")"""
    if not video_length_value:
        return "10s"  # Default
    
    # If it's a Baserow single_select object
    if isinstance(video_length_value, dict):
        # Baserow uses 'value' key for single_select display value
        value = video_length_value.get('value') or video_length_value.get('name', '10s')
        return value
    
    # If it's already a string
    return str(video_length_value)


def get_next_available_slot(scheduled_slots: List[datetime], page_id: str) -> datetime:
    """Find next available posting slot in Cambodia time (UTC+7)"""
    # Get current time in Cambodia timezone (proper way)
    now_utc = datetime.now(timezone.utc)
    now_cambodia = now_utc.astimezone(CAMBODIA_TZ)
    
    logger.info(f"DEBUG: Current Cambodia time: {now_cambodia.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check next 30 days for available slots
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
                # Keep timezone info
                return slot_time
    
    # Fallback: tomorrow first slot
    fallback = now_cambodia + timedelta(days=1)
    return fallback.replace(hour=POSTING_TIMES[0], minute=0, second=0, microsecond=0)

