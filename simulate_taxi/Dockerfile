from python:3.7-slim

COPY simulate_taxi_data.py /
COPY requirements.txt /
COPY nycity1-6d08dddde035.json /

ENV GOOGLE_APPLICATION_CREDENTIALS='nycity1-6d08dddde035.json'
RUN pip3 install -r requirements.txt

CMD ["python","simulate_taxi_data.py"]
