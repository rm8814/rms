FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Persist DB outside container — mount a volume at /data
ENV REHAT_DB_PATH=/data/rehat.db

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
