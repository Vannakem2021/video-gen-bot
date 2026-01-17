"""
Sora Bot - Gemini Caption Generator

Functions for generating viral social media captions using Google Gemini API.
"""

import aiohttp
import json

from .config import GEMINI_API_KEY, logger


async def generate_caption(video_prompt: str) -> str:
    """Generate viral caption using Gemini"""
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY not configured, using fallback caption")
        return "Check this out! 🔥\n#Viral #Trending #ForYou"
    
    # Use Gemini 2.5 Flash (fast and capable)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    system_prompt = """You are a viral social media caption expert. Generate short, engaging captions for video posts.

Rules:
1. Keep it under 150 characters (not including hashtags)
2. Use 1-2 emojis maximum
3. Include 3-5 relevant hashtags at the end
4. Make it catchy and scroll-stopping
5. Don't explain the video, create intrigue
6. Use conversational, Gen-Z friendly language
7. NO quotation marks around the caption

Format:
[Caption text with emoji]
#Hashtag1 #Hashtag2 #Hashtag3"""

    user_message = f"Generate a viral caption for this video:\n\n{video_prompt[:500]}"
    
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": f"{system_prompt}\n\n{user_message}"}]
            }
        ],
        "generationConfig": {
            "temperature": 0.9,
            "maxOutputTokens": 200
        }
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=30) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    logger.error(f"Gemini API error: {resp.status} - {error}")
                    return "Check this out! 🔥\n#Viral #Trending #ForYou"
                
                data = await resp.json()
                
                # Extract text from Gemini response
                candidates = data.get('candidates', [])
                if candidates:
                    content = candidates[0].get('content', {})
                    parts = content.get('parts', [])
                    if parts:
                        caption = parts[0].get('text', '').strip()
                        # Remove any quotation marks
                        caption = caption.strip('"').strip("'")
                        return caption
                
                return "Check this out! 🔥\n#Viral #Trending #ForYou"
                
    except Exception as e:
        logger.error(f"Gemini caption error: {e}")
        return "Check this out! 🔥\n#Viral #Trending #ForYou"
