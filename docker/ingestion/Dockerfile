FROM python:3.12-slim

WORKDIR /opt/app

COPY ingestion/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ingestion /opt/app

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
