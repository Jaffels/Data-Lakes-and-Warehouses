FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY daily_update_yfinance.py .
COPY config.py .

ENTRYPOINT ["python", "daily_update_yfinance.py"]
CMD ["all"]