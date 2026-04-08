FROM apache/spark:3.5.1

USER root
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && chown -R spark:root /home/spark

WORKDIR /opt/app
COPY processing /opt/app/processing

USER spark
