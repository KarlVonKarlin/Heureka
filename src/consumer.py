import logging
import json
import psycopg2
from typing import Optional, Any

from resources import rabbitmq_connect

DESIRED_KEYS = ['attributes', 'legacy']
LOG_FORMAT = '%(asctime)s;%(levelname)s;%(message)s'
LOG = logging.getLogger(__name__)
            
# def parse():

class Database():
    
    def __init__(self,
                 database: str = "postgres",
                 host: str = "localhost",
                 user: str = "user_name",
                 password: str = "user_password",
                 port: str = "5432"):
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        
    def connect(self) -> Any:
        conn = psycopg2.connect(database="postgres",
                                host="localhost",
                                user="user_name",
                                password="user_password",
                                port="5432")
        conn.autocommit = True
        return conn

    def execute_cmds(self, commands: list) -> None:
        for cmd in commands:
            try:
                conn = self.connect()
                cur = conn.cursor()
                cur.execute(cmd)
                cur.close()
                conn.commit()
            except(psycopg2.errors.DuplicateDatabase) as err:
                LOG.error(err)
            except(psycopg2.errors.DuplicateTable) as err:
                LOG.error(err)
            finally:
                if conn:
                    conn.close()
        
    def create_db(self):

        self.execute_cmds(['CREATE database postgres;'])

    def create_tables(self) -> None:
        commands = [
                """
                CREATE TABLE offers (
                    offerId varchar UNIQUE,
                    PRIMARY KEY (offerId)
                )
                """,
                """
                CREATE TABLE legacy (
                    offerId varchar
                        REFERENCES offers(offerId),
                    platformId varchar,
                    countryCode varchar,
                    platformSellerId decimal,
                    platformOfferId decimal,
                    platformProductId decimal,
                    isOversizeDelivery bool,
                    isDeliveryFeeByQuantity bool,
                    unitWeightGram varchar,
                    isFreeMarketplaceDelivery bool,
                    PRIMARY KEY (offerId)
                )
                """,
                """
                CREATE TABLE attributes (
                    attributesId SERIAL,
                    offerId varchar,
                    name varchar,
                    value varchar,
                    unit varchar,
                    PRIMARY KEY(attributesId),
                    FOREIGN KEY(offerId)
                        REFERENCES offers(offerId)
                )
                """
        ]
        self.execute_cmds(commands)

    def insert_offer(self, offer_id) -> None:
        print(offer_id, type(offer_id))
        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO offers (
                    offerId
                )
                VALUES (%s)
                ON CONFLICT DO NOTHING
                """,(
                    offer_id,
                )
            )
            cur.close()
            conn.commit()
        finally:
            if conn:
                conn.close()


    def insert_legacy(self, offer_id: str, legacy_dict: dict) -> None:
        print(json.dumps(legacy_dict, indent=2))
        try:
            conn = self.connect()
            cur = conn.cursor()
            cur.execute(
            """
            INSERT INTO legacy (
                offerId,
                platformId,
                countryCode,
                platformSellerId,
                platformOfferId,
                platformProductId,
                isOversizeDelivery,
                isDeliveryFeeByQuantity,
                unitWeightGram,
                isFreeMarketplaceDelivery
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
            """,(offer_id,
                 legacy_dict['platformId'],
                 legacy_dict['countryCode'],
                 legacy_dict['platformSellerId'],
                 legacy_dict['platformOfferId'],
                 legacy_dict['platformProductId'],
                 legacy_dict['isOversizeDelivery'],
                 legacy_dict['isDeliveryFeeByQuantity'],
                 legacy_dict['unitWeightGram'],
                 legacy_dict['isFreeMarketplaceDelivery']
            )
            )
            cur.close()
            conn.commit()
        finally:
            if conn:
                conn.close()

    def insert_attributes(self, offer_id: str, attributes_list: list) -> None:
        try:
            conn = self.connect()
            cur = conn.cursor()
            for element in attributes_list:
                cur.execute(
                    """
                    INSERT INTO attributes (
                        offerId,
                        name,
                        value,
                        unit
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """,(offer_id,
                         element['name'],
                         element['value'],
                         element['unit']
                        )
                )
            cur.close()
            conn.commit()
        finally:
            if conn:
                conn.close()

class Consumer():
    
    def __init__(self, db: Database, queue_name: str = 'default'):
        """Single queue consumer of messages from RabbitMQ.

            :param queue: Queue name. Defaults to 'test'.
        """
        self.db = db
        self.connection, self.channel = rabbitmq_connect(queue_name=queue_name)
        LOG.info('RabbitMQ channel opened.')

    @staticmethod
    def parse_on_receive(channel, method, properties, body):
            #   on_message_callback(channel, method, properties, body)
            # - channel: BlockingChannel
            # - method: spec.Basic.Deliver
            # - properties: spec.BasicProperties
            # - body: bytes
            
        db = Database()

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

        # for i in result_list:
        #     print(i)

    def start_consuming(self, queue_name: str, auto_ack: str = True) -> None:
        """
        
        # todo: auto_ack to false
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
    # db.create_db()
    db.create_tables()
    consumer = Consumer(db=db)
    consumer.start_consuming('default')

if __name__ == "__main__":
    main()
