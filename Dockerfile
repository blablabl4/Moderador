FROM python:3.10-slim

WORKDIR /app

# Install system dependencies if needed (e.g. for building python modules)
# RUN apt-get update && apt-get install -y gcc

# Data directory for persistence
RUN mkdir -p /data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose port for Webhook
EXPOSE 8000

# Run the application
# We use python bot.py assuming it has the uvicorn.run call, or we can use uvicorn directly.
# Using python bot.py allows for easier custom startup logic if we add it later.
CMD ["python", "bot.py"]
