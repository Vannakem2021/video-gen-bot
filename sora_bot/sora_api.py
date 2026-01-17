"""
Sora Bot - GeminiGen Sora API Client

Functions for interacting with the GeminiGen.ai Sora 2 video generation API.
"""

import aiohttp

from .config import SORA_API_KEY, logger


async def generate_video(prompt: str, duration: int = 10) -> str:
    """Call GeminiGen.ai Sora 2 API to generate video"""
    if not SORA_API_KEY:
        raise Exception("SORA_API_KEY not configured")
    
    # Validate duration
    if duration not in [10, 15]:
        duration = 10
    
    logger.info(f"🎬 Calling Sora API (sora-2, {duration}s)...")
    
    url = "https://api.geminigen.ai/uapi/v1/video-gen/sora"
    
    # API requires multipart form data, not JSON
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
    import asyncio
    
    if not SORA_API_KEY:
        raise Exception("SORA_API_KEY not configured")
    
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
