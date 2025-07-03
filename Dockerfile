FROM apache/airflow:2.7.2-python3.8
USER root
RUN apt-get update && apt-get install -y git curl && \
    pip install --upgrade pip
USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . /opt/airflow/
WORKDIR /opt/airflow
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
CMD ["airflow", "scheduler"]
