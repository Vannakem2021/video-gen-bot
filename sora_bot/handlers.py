"""
Sora Bot - Command & Webhook Handlers

Handler functions for Telegram commands and GeminiGen webhooks.
"""

import json
from datetime import datetime
from aiohttp import web

from .config import pending_jobs, logger
from .helpers import escape_markdown, parse_video_length
from .telegram_client import send_telegram_message
from .sora_api import generate_video, poll_for_completion
from .gemini_caption import generate_caption
from .baserow_client import (
    get_ready_records, update_record_status, create_post_queue_record, get_page_name
)


async def handle_generate_command(chat_id: str = None):
    """Handle /generate command - process ready records"""
    
    await send_telegram_message("🔍 Looking for records to generate...", [chat_id] if chat_id else None)
    
    try:
        ready_records, stats = await get_ready_records()
        
        if not ready_records:
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
            record_id = record.get('id')
            
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
            
            duration_str = parse_video_length(video_length)
            # Extract numeric duration from string like "15s" -> 15
            duration = int(''.join(filter(str.isdigit, duration_str)) or '10')
            
            logger.info(f"DEBUG: Video Length raw = {video_length}, parsed duration = {duration_str}")
            
            try:
                await update_record_status(record_id, 'Processing')
                
                # Call Sora API - returns uuid string directly
                uuid = await generate_video(prompt, duration)
                
                logger.info(f"✅ Generation started: {uuid}")
                
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
        # poll_for_completion returns video_url string directly
        video_url = await poll_for_completion(uuid)
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
    """Complete video generation - save to Baserow, generate caption, notify"""
    
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
        # Update Baserow with video
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
        if uuid in pending_jobs:
            del pending_jobs[uuid]


async def handle_sora_webhook(request):
    """Handle GeminiGen.ai webhook callback"""
    
    try:
        data = await request.json()
        
        # DEBUG: Log full payload to understand structure
        logger.info(f"📥 Webhook raw payload: {json.dumps(data, default=str)[:500]}")
        
        # GeminiGen uses "event_name" not "event" (confirmed from actual payload)
        event = data.get('event_name') or data.get('event')
        payload = data.get('data', {})
        uuid = payload.get('uuid')
        
        logger.info(f"📥 Webhook received: {event} for {uuid}")
        
        if event == 'VIDEO_GENERATION_COMPLETED':
            # media_url can be at top level or inside data object
            video_url = payload.get('media_url') or data.get('media_url')
            if uuid and video_url:
                await complete_video_generation(uuid, video_url)
            else:
                logger.warning(f"Missing video_url in completed webhook for {uuid}. Full data: {json.dumps(data, default=str)[:500]}")
        
        elif event == 'VIDEO_GENERATION_FAILED':
            error_msg = payload.get('error_message') or data.get('error_message') or 'Unknown error'
            error_code = payload.get('error_code') or data.get('error_code') or ''
            
            logger.error(f"❌ Video generation failed: {uuid} - {error_code}: {error_msg}")
            
            job = pending_jobs.get(uuid)
            if job:
                # Update Baserow record to Error status
                await update_record_status(job['record_id'], 'Error')
                await send_telegram_message(
                    f"❌ *Video Generation Failed*\n\n"
                    f"UUID: `{uuid[:12]}...`\n"
                    f"Error: {error_msg}\n"
                    f"Code: {error_code}",
                    [job.get('chat_id')] if job.get('chat_id') else None
                )
                del pending_jobs[uuid]
            else:
                # Job not found - still notify default chat so errors aren't lost
                logger.warning(f"Job {uuid} not in pending_jobs - sending to default chat")
                await send_telegram_message(
                    f"⚠️ *Sora Error (untracked job)*\n\n"
                    f"UUID: `{uuid[:12] if uuid else 'unknown'}...`\n"
                    f"Error: {error_msg}\n"
                    f"Code: {error_code}",
                    None  # Will use default TELEGRAM_CHAT_IDS
                )
        
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
