import sys
import logging
import json
from pathlib import Path

from heudb.db import Database
from resources.resources import rabbitmq_connect

DESIRED_KEYS = ['attributes', 'legacy']
LOG_FORMAT = '%(asctime)s;%(levelname)s;%(message)s'
LOG = logging.getLogger(__name__)

class Consumer():
    
    def __init__(self, db: Database, queue_name: str = 'default'):
        """Single queue consumer of messages from RabbitMQ.

            :param db: Database handle
            :param queue: Queue name (default: 'default').
        """
        self.db = db
        self.connection, self.channel = rabbitmq_connect(queue_name=queue_name)
        LOG.info('RabbitMQ channel opened.')

    @staticmethod
    def parse_on_receive(channel, method, properties, body):
        """Callback for RabbitMQ processing message upon arrival.

        :param channel: BlockingChannel.
        :param method: spec.Basic.Deliver.
        :param properties: spec.BasicProperties.
        :param body: Message body as bytes.
        """
            
            #   on_message_callback(channel, method, properties, body)
            # - channel: BlockingChannel
            # - method: spec.Basic.Deliver
            # - properties: spec.BasicProperties
            # - body: bytes
            
        db = Database()

        def _crawl_recursively(data: dict, match: str, id: str) -> tuple:
            """Recursive function for crawling incoming message and search
            for desired key.

            :param data: Raw message data.
            :param match: Desired key to match.
            :param id: ID of current data block.

            :yields: ID of current data block and value of matched key.
            """
            last_id = id
            for key, val in data.items():
                if key == 'id':
                    last_id = val
                if key == match:
                    yield last_id, val
                elif isinstance(val, dict):
                    for sub_dict in _crawl_recursively(val, match, last_id):
                        yield last_id, sub_dict
                elif isinstance(val, list):
                    for element in val:
                        if isinstance(element, dict):
                            for el in _crawl_recursively(element, match, last_id):
                                # print(id)
                                yield last_id, el

        msg_dict = json.loads(body.decode('utf-8'))
        for item in DESIRED_KEYS:
            for _, val in _crawl_recursively(msg_dict, item, ''):
                if val[1]:
                    db.insert_offer(val[0])
                    match item:
                        case 'attributes':
                            db.insert_attributes(offer_id=val[0], attributes_list=val[1])
                        case 'legacy':
                            db.insert_legacy(val[0], val[1])
                        case _:
                            LOG.warning('Unrecognized key!')

    def start_consuming(self, queue_name: str, auto_ack: str = True) -> None:
        """Start basic_consume of RabbitMQ.
        
        :param on_message_callback: Callback for RabbitMQ processing message upon arrival.
        :param queue: RabbitMQ queue name.
        :param auto_ack: Automatic acknowledge mode. If true, message is considered to be
        sucessfully delivered immediately after it is sent.
        """
        self.channel.basic_consume(on_message_callback=self.parse_on_receive,
                                   queue=queue_name,
                                   auto_ack=auto_ack)
        LOG.info('Consumer initiated.')
        LOG.info('Consumer started. Waiting for messages.\n...')
        self.channel.start_consuming() 

def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    db = Database()
    db.create_tables()
    consumer = Consumer(db=db)
    consumer.start_consuming('default')

if __name__ == "__main__":
    main()
