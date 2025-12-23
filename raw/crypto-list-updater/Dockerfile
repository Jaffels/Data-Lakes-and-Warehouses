FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate_crypto_list.py .

CMD ["python", "generate_crypto_list.py"]