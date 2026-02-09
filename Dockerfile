FROM apache/spark-py:v3.3.0
USER root
COPY gold_route_merge.py /opt/spark/work-dir/
# 銀行環境通常會預裝特定的 Connector
RUN pip install pyspark
USER spark
