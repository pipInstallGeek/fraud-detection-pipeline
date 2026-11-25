FROM alpine:3.19 AS jar-downloader

RUN apk add --no-cache curl

WORKDIR /jars

RUN curl -sLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

FROM apache/spark:3.5.0

USER root

COPY --from=jar-downloader /jars/*.jar /opt/spark/jars/

WORKDIR /app

RUN pip install --no-cache-dir \
    boto3 \
    "numpy<2" \
    mlflow==2.10.0 \
    pandas \
    plotly \
    pyspark==3.5.0 \
    pyarrow \
    streamlit \
    streamlit-autorefresh

COPY app.py /app/app.py

EXPOSE 8501

CMD ["streamlit", "run", "/app/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
