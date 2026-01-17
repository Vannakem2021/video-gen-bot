"""
Sora Bot - Baserow Client

Functions for interacting with the Baserow API (database operations).
"""

import json
import aiohttp
from datetime import datetime, timedelta, timezone

from .config import (
    BASEROW_URL, BASEROW_USERNAME, BASEROW_PASSWORD,
    REELS_GENERATION_TABLE, POST_QUEUE_TABLE, FACEBOOK_PAGES_TABLE,
    REELS_STATUS_OPTIONS, POST_QUEUE_STATUS_OPTIONS,
    logger
)
from .helpers import get_next_available_slot

# Global token cache (module-level)
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


def clear_token_cache():
    """Clear the token cache to force re-authentication"""
    global _baserow_token, _token_expiry
    _baserow_token = None
    _token_expiry = None


async def get_record_by_uuid(uuid: str):
    """Find a Reels Generation record by its Generation UUID field"""
    url = f"{BASEROW_URL}/api/database/rows/table/{REELS_GENERATION_TABLE}/"
    params = {
        'user_field_names': 'true',
        'filter__Generation UUID__equal': uuid,
        'size': 1
    }
    
    for attempt in range(2):
        try:
            headers = await get_baserow_headers()
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 401 and attempt == 0:
                        clear_token_cache()
                        continue
                    
                    if resp.status != 200:
                        return None
                    
                    data = await resp.json()
                    results = data.get('results', [])
                    if results:
                        return results[0]
                    return None
        except Exception as e:
            logger.error(f"Error looking up record by UUID {uuid}: {e}")
            return None
    return None


async def get_records_by_status(status: str):
    """Get all records with a specific status (for recovery/cleanup)"""
    url = f"{BASEROW_URL}/api/database/rows/table/{REELS_GENERATION_TABLE}/"
    
    status_id = REELS_STATUS_OPTIONS.get(status)
    params = {
        'user_field_names': 'true',
        'size': 100
    }
    
    # Baserow filter for single_select by value
    if status_id:
        params['filter__Status__single_select_equal'] = status_id
    
    for attempt in range(2):
        try:
            headers = await get_baserow_headers()
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 401 and attempt == 0:
                        clear_token_cache()
                        continue
                    
                    if resp.status != 200:
                        error = await resp.text()
                        logger.error(f"Failed to get records by status: {resp.status} - {error}")
                        return []
                    
                    data = await resp.json()
                    return data.get('results', [])
        except Exception as e:
            logger.error(f"Error fetching records by status {status}: {e}")
            return []
    return []


async def save_generation_uuid(record_id: int, uuid: str) -> bool:
    """Save the generation UUID to a Baserow record for crash recovery"""
    url = f"{BASEROW_URL}/api/database/rows/table/{REELS_GENERATION_TABLE}/{record_id}/"
    update_fields = {'Generation UUID': uuid}
    
    for attempt in range(2):
        try:
            headers = await get_baserow_headers()
            async with aiohttp.ClientSession() as session:
                async with session.patch(url, headers=headers, json=update_fields, params={'user_field_names': 'true'}) as resp:
                    if resp.status in [200, 201]:
                        logger.info(f"✅ Saved UUID {uuid[:12]}... to record {record_id}")
                        return True
                    elif resp.status == 401 and attempt == 0:
                        clear_token_cache()
                        continue
                    else:
                        error_text = await resp.text()
                        logger.error(f"Failed to save UUID: {resp.status} - {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Error saving UUID to record {record_id}: {e}")
            return False
    return False

async def get_page_name(page_id) -> str:
    """Lookup page name from Facebook Pages table"""
    if not page_id:
        return 'Unknown'
    
    try:
        url = f"{BASEROW_URL}/api/database/rows/table/{FACEBOOK_PAGES_TABLE}/{page_id}/"
        headers = await get_baserow_headers()
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params={'user_field_names': 'true'}) as resp:
                if resp.status != 200:
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
    global _baserow_token, _token_expiry
    
    url = f"{BASEROW_URL}/api/database/rows/table/{REELS_GENERATION_TABLE}/"
    
    params = {
        'user_field_names': 'true',
        'size': 200
    }
    
    stats = {
        'total': 0,
        'not_ready': 0,
        'not_draft': 0,
        'no_prompt': 0,
        'no_target': 0,
        'ready': 0
    }
    
    for attempt in range(2):  # Retry once on 401
        try:
            headers = await get_baserow_headers()
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 401 and attempt == 0:
                        logger.warning("Token expired in get_ready_records, refreshing...")
                        clear_token_cache()
                        continue
                    
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
                        ready_to_gen = record.get('Ready To Generate', False)
                        if isinstance(ready_to_gen, dict):
                            ready_to_gen = ready_to_gen.get('value', False)
                        
                        status_field = record.get('Status', {})
                        if isinstance(status_field, dict):
                            status = status_field.get('value', '')
                        else:
                            status = str(status_field) if status_field else ''
                        
                        has_prompt = bool(record.get('Prompt'))
                        target_page = record.get('Target Page', [])
                        has_target = bool(target_page and len(target_page) > 0)
                        
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
    
    return [], stats


async def update_record_status(record_id, status: str, video_url: str = None):
    """Update Baserow record status with video upload and retry on token expiry"""
    global _baserow_token, _token_expiry
    
    url = f"{BASEROW_URL}/api/database/rows/table/{REELS_GENERATION_TABLE}/{record_id}/"
    
    status_id = REELS_STATUS_OPTIONS.get(status)
    if status_id:
        update_fields = {'Status': status_id}
    else:
        logger.warning(f"Unknown status '{status}', using string fallback")
        update_fields = {'Status': status}
    
    if video_url:
        file_token = await upload_video_to_baserow(video_url, record_id)
        if file_token:
            update_fields['Video'] = [{'name': file_token}]
        update_fields['Ready To Generate'] = False
    
    for attempt in range(2):
        try:
            headers = await get_baserow_headers()
            async with aiohttp.ClientSession() as session:
                async with session.patch(url, headers=headers, json=update_fields, params={'user_field_names': 'true'}) as resp:
                    if resp.status in [200, 201]:
                        logger.info(f"✅ Updated record {record_id}: Status={status}")
                        return True
                    elif resp.status == 401 and attempt == 0:
                        logger.warning("Token expired, refreshing...")
                        clear_token_cache()
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
    global _baserow_token, _token_expiry
    
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
    
    create_payload = {
        'Source Reel': str(source_record_id) if source_record_id else '',
        'Page Name': [int(page_id)] if page_id else [],
        'Video URL': video_url,
        'Schedule For': schedule_time.isoformat(),
        'Status': POST_QUEUE_STATUS_OPTIONS.get('Scheduled', 3060),
        'Caption': caption
    }
    
    logger.info(f"DEBUG: Creating Post Queue record with payload: {json.dumps(create_payload, default=str)}")
    
    for attempt in range(2):
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
                        clear_token_cache()
                        continue
                    else:
                        error_text = await resp.text()
                        logger.error(f"Failed to create post queue record: {resp.status} - {error_text}")
                        return None, schedule_time
        except Exception as e:
            logger.error(f"Error creating post queue record: {e}")
            return None, schedule_time
    
    return None, schedule_time
