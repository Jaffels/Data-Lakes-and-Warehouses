FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch_cryptocompare.py .
COPY config.py .

ENTRYPOINT ["python", "fetch_cryptocompare.py"]