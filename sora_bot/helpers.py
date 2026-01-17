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
    now = datetime.now(CAMBODIA_TZ)
    
    # Start checking from today
    check_date = now.date()
    
    # Check next 30 days for available slots
    for _ in range(30):
        for hour in POSTING_TIMES:
            slot = datetime.combine(
                check_date, 
                datetime.min.time().replace(hour=hour), 
                tzinfo=CAMBODIA_TZ
            )
            
            # Skip slots in the past
            if slot <= now:
                continue
            
            # Check if slot is already taken
            slot_taken = False
            for scheduled in scheduled_slots:
                if isinstance(scheduled, str):
                    try:
                        scheduled = datetime.fromisoformat(scheduled.replace('Z', '+00:00'))
                    except:
                        continue
                
                if scheduled.astimezone(CAMBODIA_TZ).replace(minute=0, second=0, microsecond=0) == slot:
                    slot_taken = True
                    break
            
            if not slot_taken:
                return slot
        
        check_date += timedelta(days=1)
    
    # Fallback: 24 hours from now
    return now + timedelta(hours=24)
