FROM apache/airflow:2.9.0
COPY ./requirements.txt /tmp
RUN python -m pip install --upgrade pip && \
   pip install --no-cache-dir -r /tmp/requirements.txt