FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY parking_monitor.py .

CMD ["python", "parking_monitor.py"]