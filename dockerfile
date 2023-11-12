FROM python:3.10

# RUN apt-get update && apt-get install python3.10
ADD . .
RUN pip install .
RUN pip install rabbitmq
RUN pip install pika
RUN pip install pytest

CMD ["python3", "./src/consumer/consumer.py"]