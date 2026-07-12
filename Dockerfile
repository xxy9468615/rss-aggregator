FROM python:3.12-slim

# System deps for headless Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-liberation fonts-noto-cjk \
    libgtk-3-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 \
    libxshmfence1 libxss1 libxtst6 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Download Playwright Chromium binary only (system deps pre-installed above)
RUN playwright install chromium

# Cache persists to /app/data on Railway's filesystem (survives restarts)
ENV DATA_DIR=/app/data

EXPOSE 8000
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
