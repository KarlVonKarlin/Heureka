import pika

def rabbitmq_connect(host: str = 'localhost', queue_name: str = 'default'):
    """
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name)
    return connection, channel

