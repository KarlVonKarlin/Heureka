import pika
import logging

def rabbitmq_connect(where: str = 'localhost', queue_name: str = 'default'):
    connection = pika.BlockingConnection(pika.ConnectionParameters(where))
    channel = connection.channel()

    # pokus o nove vytvoreni fronty ve skutecnosti neovlivni jiz existujici frontu
    channel.queue_declare(queue=queue_name)
    return connection, channel
