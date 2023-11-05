import logging
import json
import psycopg2

from resources import rabbitmq_connect

DESIRED_KEYS = ['attributes', 'legacy']

            
# def parse():
    

class Consumer():
    
    def __init__(self, queue_name: str = 'default'):
        """Single queue consumer of messages from RabbitMQ.

            :param queue: Queue name. Defaults to 'test'.
        """
        logging.basicConfig(level=logging.INFO, format='%(asctime)s;%(levelname)s;%(message)s')
        self.log = logging.getLogger()
        self.connection, self.channel = rabbitmq_connect(queue_name=queue_name)
        self.log.info('RabbitMQ channel opened.')

    @staticmethod
    def parse_on_receive(channel, method, properties, body):
            #   on_message_callback(channel, method, properties, body)
            # - channel: BlockingChannel
            # - method: spec.Basic.Deliver
            # - properties: spec.BasicProperties
            # - body: bytes
            
        def _crawl_recursively(data: dict, match: str):
            for key, val in data.items():
                if key == match:
                    yield val
                elif isinstance(val, dict):
                    for di in _crawl_recursively(val, match):
                        yield di
                elif isinstance(val, list):
                    for element in val:
                        if isinstance(element, dict):
                            for el in _crawl_recursively(element, match):
                                yield el
                    
        msg_dict = json.loads(body.decode('utf-8'))
        for item in DESIRED_KEYS:
            result_list = list()
            print('calling recursion1')
            for val in _crawl_recursively(msg_dict, item):
                result_list.append(val)
                
            print(result_list)
            

        
            
        
    def start_consuming(self, queue_name: str, auto_ack: str = True) -> None:
        """
        """
        self.channel.basic_consume(on_message_callback=self.parse_on_receive,
                                   queue=queue_name,
                                   auto_ack=auto_ack)
        self.log.info('Consumer initiated.')
        self.log.info('Consumer started. Waiting for messages.\n...')
        self.channel.start_consuming()

def main():
    consumer = Consumer()
    consumer.start_consuming('default')

if __name__ == "__main__":
    main()
