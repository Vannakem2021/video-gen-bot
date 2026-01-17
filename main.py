#!/usr/bin/env python3
"""
Sora Telegram Bot - Entry Point

Run this file to start the bot:
    python main.py

Or with Docker:
    docker-compose up -d
"""

import asyncio
from sora_bot import main

if __name__ == "__main__":
    asyncio.run(main())
