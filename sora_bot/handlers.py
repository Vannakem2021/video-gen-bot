"""
Sora Bot - Command & Webhook Handlers

Handler functions for Telegram commands and GeminiGen webhooks.
"""

import json
import asyncio
from datetime import datetime, timezone, timedelta
from aiohttp import web

from .config import pending_jobs, logger
from .helpers import escape_markdown, parse_video_length
from .telegram_client import send_telegram_message
from .sora_api import generate_video, poll_for_completion, check_job_status
from .gemini_caption import generate_caption
from .baserow_client import (
    get_ready_records, update_record_status, create_post_queue_record, get_page_name,
    get_record_by_uuid, get_records_by_status, save_generation_uuid
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
                
                # Save UUID to Baserow for crash recovery
                await save_generation_uuid(record_id, uuid)
                
                pending_jobs[uuid] = {
                    'record_id': record_id,
                    'prompt': prompt,
                    'page_id': page_id,
                    'page_name': page_name,
                    'status': 'generating',
                    'chat_id': chat_id,
                    'started_at': datetime.now(timezone.utc).isoformat()
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
    
    # Fallback: If not in memory, look up from Baserow (crash recovery)
    if not job:
        logger.info(f"Job {uuid} not in memory, checking Baserow...")
        record = await get_record_by_uuid(uuid)
        if record:
            # Reconstruct job info from Baserow record
            target_page = record.get('Target Page', [])
            if isinstance(target_page, list) and len(target_page) > 0:
                page_id = target_page[0].get('id') if isinstance(target_page[0], dict) else target_page[0]
                page_name = target_page[0].get('value', 'Unknown') if isinstance(target_page[0], dict) else 'Unknown'
            else:
                page_id = None
                page_name = 'Unknown'
            
            job = {
                'record_id': record['id'],
                'prompt': record.get('Prompt', ''),
                'page_id': page_id,
                'page_name': page_name,
                'chat_id': None  # Lost on crash, will use default chat IDs
            }
            logger.info(f"✅ Recovered job from Baserow: record_id={record['id']}")
        else:
            logger.warning(f"No pending job found for {uuid} (not in memory or Baserow)")
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
            
            # Fallback: If not in memory, look up from Baserow (crash recovery)
            if not job and uuid:
                logger.info(f"Job {uuid} not in memory for failure handling, checking Baserow...")
                record = await get_record_by_uuid(uuid)
                if record:
                    job = {
                        'record_id': record['id'],
                        'prompt': record.get('Prompt', ''),
                        'chat_id': None
                    }
                    logger.info(f"✅ Recovered failed job from Baserow: record_id={record['id']}")
            
            if job:
                # Update Baserow record to Error status
                await update_record_status(job['record_id'], 'Error')
                await send_telegram_message(
                    f"❌ *Video Generation Failed*\n\n"
                    f"UUID: `{uuid[:12]}...`\n"
                    f"Error: {escape_markdown(error_msg)}\n"
                    f"Code: {error_code}",
                    [job.get('chat_id')] if job.get('chat_id') else None
                )
                if uuid in pending_jobs:
                    del pending_jobs[uuid]
            else:
                # Job not found anywhere - still notify default chat so errors aren't lost
                logger.warning(f"Job {uuid} not found in memory or Baserow - sending to default chat")
                await send_telegram_message(
                    f"⚠️ *Sora Error (untracked job)*\n\n"
                    f"UUID: `{uuid[:12] if uuid else 'unknown'}...`\n"
                    f"Error: {escape_markdown(error_msg)}\n"
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


async def recover_pending_jobs():
    """On startup, check for records stuck in 'Processing' status and resume polling"""
    logger.info("🔄 Checking for pending jobs to recover...")
    
    try:
        processing_records = await get_records_by_status('Processing')
        
        if not processing_records:
            logger.info("✅ No stuck jobs to recover")
            return
        
        logger.info(f"📋 Found {len(processing_records)} records in Processing status")
        
        for record in processing_records:
            uuid = record.get('Generation UUID')
            if not uuid:
                logger.warning(f"Record {record['id']} has no UUID, marking as Error")
                await update_record_status(record['id'], 'Error')
                continue
            
            # Try to poll for completion
            logger.info(f"🔄 Recovering job {uuid[:12]}... for record {record['id']}")
            
            # Reconstruct job info
            target_page = record.get('Target Page', [])
            if isinstance(target_page, list) and len(target_page) > 0:
                page_id = target_page[0].get('id') if isinstance(target_page[0], dict) else target_page[0]
                page_name = target_page[0].get('value', 'Unknown') if isinstance(target_page[0], dict) else 'Unknown'
            else:
                page_id = None
                page_name = 'Unknown'
            
            pending_jobs[uuid] = {
                'record_id': record['id'],
                'prompt': record.get('Prompt', ''),
                'page_id': page_id,
                'page_name': page_name,
                'status': 'recovering',
                'chat_id': None,
                'started_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Start polling in background
            asyncio.create_task(poll_and_complete(uuid))
        
        await send_telegram_message(
            f"🔄 *Bot Restarted*\n"
            f"Recovering {len(processing_records)} pending job(s)...",
            None  # Use default chat IDs
        )
        
    except Exception as e:
        logger.error(f"Error during job recovery: {e}")


async def cleanup_stale_jobs():
    """Background task to check on Processing jobs and handle completed/failed ones
    
    Runs every 10 minutes to:
    1. Poll GeminiGen API for actual status of jobs
    2. Complete any jobs that finished but webhook was missed
    3. Mark failed jobs as Error with notification
    4. Mark jobs >30 min old as Error (timeout)
    """
    CHECK_INTERVAL_SECONDS = 600  # Run every 10 minutes
    STALE_THRESHOLD_MINUTES = 30
    
    while True:
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
        
        try:
            logger.info("🔍 Checking status of Processing jobs...")
            
            processing_records = await get_records_by_status('Processing')
            now = datetime.now(timezone.utc)
            
            completed_count = 0
            failed_count = 0
            timeout_count = 0
            
            for record in processing_records:
                uuid = record.get('Generation UUID')
                record_id = record['id']
                
                if not uuid:
                    # No UUID - can't check, mark as error after timeout
                    logger.warning(f"⚠️ Record {record_id} has no UUID, marking as Error")
                    await update_record_status(record_id, 'Error')
                    await send_telegram_message(
                        f"⚠️ *Job Error*\nRecord {record_id} stuck without UUID",
                        None
                    )
                    timeout_count += 1
                    continue
                
                # Poll GeminiGen API for actual status
                logger.info(f"🔍 Checking status for {uuid[:12]}...")
                status_info = await check_job_status(uuid)
                api_status = status_info.get('status', 0)
                
                if api_status == 2:  # Completed!
                    logger.info(f"✅ Job {uuid[:12]}... completed (webhook missed)")
                    video_url = status_info.get('media_url')
                    if video_url:
                        await complete_video_generation(uuid, video_url)
                        completed_count += 1
                    else:
                        # Completed but no URL - check raw data
                        raw = status_info.get('raw', {})
                        generated = raw.get('generated_video', [])
                        if generated:
                            video_url = generated[0].get('video_url') or generated[0].get('file_download_url')
                            if video_url:
                                await complete_video_generation(uuid, video_url)
                                completed_count += 1
                                continue
                        
                        logger.error(f"Job {uuid[:12]}... completed but no video URL found")
                        await update_record_status(record_id, 'Error')
                        await send_telegram_message(
                            f"⚠️ *Job Completed but No Video URL*\nUUID: `{uuid[:12]}...`",
                            None
                        )
                        failed_count += 1
                
                elif api_status == 3:  # Failed!
                    error_msg = status_info.get('error_message') or 'Unknown error'
                    logger.error(f"❌ Job {uuid[:12]}... failed: {error_msg}")
                    
                    await update_record_status(record_id, 'Error')
                    await send_telegram_message(
                        f"❌ *Video Generation Failed*\n\n"
                        f"UUID: `{uuid[:12]}...`\n"
                        f"Error: {escape_markdown(error_msg)}",
                        None
                    )
                    
                    if uuid in pending_jobs:
                        del pending_jobs[uuid]
                    failed_count += 1
                
                elif api_status == 1:  # Still processing
                    # Check if it's been too long (timeout)
                    job = pending_jobs.get(uuid)
                    if job and job.get('started_at'):
                        try:
                            started_at = datetime.fromisoformat(job['started_at'])
                            age_minutes = (now - started_at).total_seconds() / 60
                            if age_minutes > STALE_THRESHOLD_MINUTES:
                                logger.warning(f"⏰ Job {uuid[:12]}... timed out ({age_minutes:.0f} min)")
                                await update_record_status(record_id, 'Error')
                                await send_telegram_message(
                                    f"⏰ *Job Timed Out*\n\n"
                                    f"UUID: `{uuid[:12]}...`\n"
                                    f"Age: {age_minutes:.0f} minutes",
                                    None
                                )
                                del pending_jobs[uuid]
                                timeout_count += 1
                        except:
                            pass
                    else:
                        logger.info(f"⏳ Job {uuid[:12]}... still processing ({status_info.get('status_percentage', 0)}%)")
                
                else:
                    # Unknown status - log but don't fail
                    logger.warning(f"❓ Job {uuid[:12]}... has unknown status: {api_status}")
            
            # Summary
            total = completed_count + failed_count + timeout_count
            if total > 0:
                await send_telegram_message(
                    f"🔍 *Job Status Check Complete*\n"
                    f"✅ Completed: {completed_count}\n"
                    f"❌ Failed: {failed_count}\n"
                    f"⏰ Timeout: {timeout_count}",
                    None
                )
            logger.info(f"🔍 Status check complete: {completed_count} completed, {failed_count} failed, {timeout_count} timeout")
                
        except Exception as e:
            logger.error(f"Error during job status check: {e}")
            import traceback
            traceback.print_exc()


