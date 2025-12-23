FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY fetch_yfinance.py .
COPY config.py .

# Run the script
ENTRYPOINT ["python", "fetch_yfinance.py"]
CMD ["all"]