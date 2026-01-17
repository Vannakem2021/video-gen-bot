"""
Sora Bot - GeminiGen Sora API Client

Functions for interacting with the GeminiGen.ai Sora 2 video generation API.
"""

import aiohttp

from .config import SORA_API_KEY, logger


async def generate_video(prompt: str, duration: int = 10) -> dict:
    """Call GeminiGen.ai Sora 2 API to generate video"""
    if not SORA_API_KEY:
        raise Exception("SORA_API_KEY not configured")
    
    url = "https://api.geminigen.ai/v1/video/new"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SORA_API_KEY}"
    }
    
    payload = {
        "model_name": "sora-2",
        "duration": duration,
        "prompt": prompt
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as resp:
            if resp.status != 200:
                error = await resp.text()
                raise Exception(f"Sora API error: {resp.status} - {error}")
            
            return await resp.json()


async def poll_for_completion(uuid: str, max_attempts: int = 30, delay_seconds: int = 15) -> dict:
    """Poll for video completion (fallback if webhook not working)"""
    import asyncio
    
    if not SORA_API_KEY:
        raise Exception("SORA_API_KEY not configured")
    
    url = f"https://api.geminigen.ai/v1/video/{uuid}/status"
    
    headers = {
        "Authorization": f"Bearer {SORA_API_KEY}"
    }
    
    for attempt in range(max_attempts):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        logger.warning(f"Poll attempt {attempt + 1} failed: {resp.status}")
                        await asyncio.sleep(delay_seconds)
                        continue
                    
                    data = await resp.json()
                    status = data.get('status')
                    
                    if status == 2:  # Completed
                        return {
                            'completed': True,
                            'media_url': data.get('media_url'),
                            'data': data
                        }
                    elif status == 3:  # Failed
                        return {
                            'completed': False,
                            'error': data.get('error_message', 'Unknown error'),
                            'data': data
                        }
                    else:
                        # Still processing
                        progress = data.get('status_percentage', 0)
                        logger.info(f"Poll {attempt + 1}/{max_attempts}: {progress}%")
                        await asyncio.sleep(delay_seconds)
                        
        except Exception as e:
            logger.error(f"Poll error: {e}")
            await asyncio.sleep(delay_seconds)
    
    return {
        'completed': False,
        'error': f'Timeout after {max_attempts * delay_seconds} seconds',
        'data': None
    }
