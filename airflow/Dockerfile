FROM apache/airflow:2.8.1

USER airflow

#RUN #pip install apache-airflow-providers-apache-spark

RUN pip install \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt \
    apache-airflow-providers-apache-spark

# Patch Flask-Session timezone bug
RUN sed -i 's/datetime.utcnow()/datetime.now(saved_session.expiry.tzinfo)/g' \
    /home/airflow/.local/lib/python3.8/site-packages/flask_session/sessions.py