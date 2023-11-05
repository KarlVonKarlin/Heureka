import json
import logging
from pathlib import Path

from resources import rabbitmq_connect


class Producer:

    def __init__(self, queue_name: str = 'default'):
        """Message producer for RabbitMQ broker.
        """
        logging.basicConfig(level=logging.INFO, format='%(asctime)s;%(levelname)s;%(message)s')
        self.log = logging.getLogger()
        self.connection, self.channel = rabbitmq_connect(queue_name=queue_name)
        self.log.info('RabbitMQ channel opened.')
        
    def prepare_msg_from_file(self, json_path: Path) -> str:
        self.log.info(f'Preparing json from: {json_path}')
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
    producer = Producer()
    json_path = Path(__file__).parent / 'mock_offers.json'
    producer.publish(producer.prepare_msg_from_file(json_path), routing_key='default')
    producer.close_connection()

if __name__ == "__main__":
    main()
