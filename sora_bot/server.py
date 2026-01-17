"""
Sora Bot - Server Entry Point

Main entry point that starts the webhook server and Telegram bot.
"""

import asyncio
from aiohttp import web

from .config import (
    TELEGRAM_BOT_TOKEN, SORA_API_KEY, GEMINI_API_KEY,
    BASEROW_URL, BASEROW_USERNAME, BASEROW_PASSWORD,
    TELEGRAM_CHAT_IDS, WEBHOOK_HOST, WEBHOOK_PORT, WEBHOOK_PATH,
    logger
)
from .handlers import handle_sora_webhook, health_check, handle_generate_command, recover_pending_jobs, cleanup_stale_jobs
from .telegram_client import send_telegram_with_keyboard, handle_status_command


async def handle_telegram_message(text: str, chat_id: str):
    """Route incoming Telegram messages to appropriate handlers"""
    text_lower = text.lower().strip()
    
    if text_lower in ['/generate', '🎬 generate', '/gen']:
        await handle_generate_command(chat_id)
    elif text_lower in ['/status', '📊 status']:
        await handle_status_command(chat_id)
    elif text_lower in ['/help', '❓ help', '/start']:
        await send_telegram_with_keyboard(
            "🎬 *Sora Video Bot*\n\n"
            "Commands:\n"
            "• 🎬 Generate - Process ready records\n"
            "• 📊 Status - Check bot status\n"
            "• ❓ Help - Show this message\n\n"
            "Use the buttons below or type commands.",
            chat_id
        )
    else:
        # Ignore other messages or show help
        if text_lower.startswith('/'):
            await send_telegram_with_keyboard("Unknown command. Use /help for available commands.", chat_id)


async def poll_telegram_updates():
    """Long-poll for Telegram updates"""
    import aiohttp
    
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN not configured, skipping Telegram bot")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    offset = 0
    
    logger.info("📱 Starting Telegram bot polling...")
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, params={'offset': offset, 'timeout': 30}) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(5)
                        continue
                    
                    data = await resp.json()
                    
                    for update in data.get('result', []):
                        offset = update['update_id'] + 1
                        
                        message = update.get('message', {})
                        text = message.get('text', '')
                        chat_id = str(message.get('chat', {}).get('id', ''))
                        
                        if not chat_id:
                            continue
                        
                        await handle_telegram_message(text, chat_id)
                        
            except Exception as e:
                logger.error(f"Telegram polling error: {e}")
                await asyncio.sleep(5)


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
    
    # Recover any pending jobs from before restart
    await recover_pending_jobs()
    
    # Start stale job cleanup background task
    asyncio.create_task(cleanup_stale_jobs())
    logger.info("🧹 Stale job cleanup task started (runs every 30 min)")
    
    # Start Telegram polling
    await poll_telegram_updates()


if __name__ == "__main__":
    asyncio.run(main())
