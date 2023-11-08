import json
import logging
from pathlib import Path

from resources import rabbitmq_connect

LOG_FORMAT = '%(asctime)s;%(levelname)s;%(message)s'
LOG = logging.getLogger(__name__)

class Producer:

    def __init__(self, queue_name: str = 'default'):
        """Message producer for RabbitMQ broker.
        """
        LOG = logging.getLogger()
        self.connection, self.channel = rabbitmq_connect(queue_name=queue_name)
        LOG.info('RabbitMQ channel opened.')
        
    def prepare_msg_from_file(self, json_path: Path) -> str:
        LOG.info(f'Preparing json from: {json_path}')
        with open(json_path, 'r', encoding='utf-8') as json_file:
            json_bytes = json.dumps(json.load(json_file)).encode('utf-8')
            return json_bytes
        
    def publish(self, body: str, exchange: str = '', routing_key: str = 'default') -> None:
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=routing_key,
                                   body=body)

    def close_connection(self) -> None:
        self.connection.close()
   
def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    producer = Producer()
    json_path = Path(__file__).parent / 'mock_offers.json'
    producer.publish(producer.prepare_msg_from_file(json_path), routing_key='default')
    producer.close_connection()

if __name__ == "__main__":
    main()
