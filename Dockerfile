FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the sora_bot package and entry point
COPY sora_bot/ ./sora_bot/
COPY main.py .

# Expose webhook port
EXPOSE 8080

# Run bot
CMD ["python", "main.py"]
