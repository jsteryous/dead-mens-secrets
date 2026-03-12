FROM python:3.11-slim

# Install FFmpeg and fonts
RUN apt-get update && apt-get install -y \
    ffmpeg \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Unbuffered output — logs appear in real time in Railway
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "generate_video.py"]