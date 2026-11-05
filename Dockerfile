FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ladder_manager.py bot.py ./
ENV PYTHONUNBUFFERED=1 DATA_DIR=/data
VOLUME ["/data"]
CMD ["python", "bot.py"]
