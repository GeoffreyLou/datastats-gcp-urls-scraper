FROM python:3.11-slim

COPY . /app
WORKDIR /app

# Get Chrome and dependancies for Selenium
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' \
    && apt-get update && apt-get install -y \
    google-chrome-stable \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv
RUN uv venv && uv sync
ENV PATH="app/.venv/bin:$PATH"

# Run Python with uv
ENTRYPOINT ["uv", "run", "src/main.py"]
