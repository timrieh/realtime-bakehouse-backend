FROM python:3.12-slim

WORKDIR /opt/app

COPY producer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY producer /opt/app

CMD ["python", "sensor_simulator.py"]
