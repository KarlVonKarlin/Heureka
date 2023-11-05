import logging
import json
import psycopg2

from resources import rabbitmq_connect

DESIRED_KEYS = ['attributes', 'legacy']

            
# def parse():

class Database():
    
    def __init__(self):
        self.conn = psycopg2.connect(database="postgres",
                                host="localhost",
                                user="user_name",
                                password="user_password",
                                port="5432")
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        
    def build_db(self):

        #Preparing query to create a database
        try:
            self.cur.execute('CREATE database heureka;')
        except psycopg2.errors.DuplicateDatabase:
            print('Database already exists!')
        
        try:
            self.cur.execute('CREATE TABLE offers ('
                            'id varchar UNIQUE,'
                            'PRIMARY KEY (id)'
                            ')') 
        except(psycopg2.errors.DuplicateTable):
            print('Table offers already exists!')
        
        try:
            self.cur.execute('CREATE TABLE legacy ('
                        'id varchar REFERENCES offers(id),'
                        'platformId varchar,'
                        'countryCode varchar,'
                        'platformSellerId integer,'
                        'platformOfferId integer,'
                        'platformProductId integer,'
                        'isOversizeDelivery bool,'
                        'isDeliveryFeeByQuantity bool,'
                        'unitWeightGram varchar,'
                        'isFreeMarketplaceDelivery bool,'
                        'PRIMARY KEY (id))')
        except(psycopg2.errors.DuplicateTable):
            print('Table legacy already exists!')

        try:
            self.cur.execute('CREATE TABLE attributes ('
                        'attributes_id SERIAL,'
                        'offer_id varchar,'
                        'name varchar,'
                        'value varchar,'
                        'unit varchar,'
                        'PRIMARY KEY(attributes_id),'
                        'CONSTRAINT fk_offer FOREIGN KEY(offer_id)'
                        'REFERENCES offers(id)'
                        ')')
        except(psycopg2.errors.DuplicateTable):
            print('Table attributes already exists!')
        self.conn.commit()
    
    def insert_attributes(self, attributes):
        self.cur.execute(f'INSERT INTO attributes VALUES (%s, %s) ')
    
    def close_db(self):
        self.cur.close()
        self.conn.close()    

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

        def _crawl_recursively(data: dict, match: str, id: str):
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
                                print(id)
                                yield last_id, el

        msg_dict = json.loads(body.decode('utf-8'))
        for item in DESIRED_KEYS:
            result_list = list()
            for id, val in _crawl_recursively(msg_dict, item, ''):
                result_list.append((id, val))
                print('append', id, val)

        for i in result_list:
            print(i)

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
    db = Database()
    db.build_db()
    consumer = Consumer()
    consumer.start_consuming('default')

if __name__ == "__main__":
    main()
