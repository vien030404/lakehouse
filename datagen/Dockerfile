# datagen: sinh sự kiện đơn hàng Olist liên tục vào Redpanda (xem datagen.py)
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY datagen.py .

# Tắt bộ đệm stdout để log hiện ngay trong `docker logs`
ENV PYTHONUNBUFFERED=1
CMD ["python", "datagen.py"]
