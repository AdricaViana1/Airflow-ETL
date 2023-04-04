FROM apache/airflow:2.5.2
COPY requirements.txt .
RUN pip install apache-airflow[selenium]
RUN pip install webdriver_manager
RUN pip install chromedriver-py
RUN pip install -r requirements.txt

