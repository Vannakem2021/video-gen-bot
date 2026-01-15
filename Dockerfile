FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot script
COPY sora-telegram-bot.py .

# Expose webhook port
EXPOSE 8080

# Run bot
CMD ["python", "sora-telegram-bot.py"]
