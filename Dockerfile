FROM apache/airflow:2.6.3

WORKDIR /opt/airflow

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

ENTRYPOINT [ "airflow" ]