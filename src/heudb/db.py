import logging
import psycopg2
import os
from typing import Any

LOG_FORMAT = '%(asctime)s;%(levelname)s;%(message)s'
LOG = logging.getLogger(__name__)

class Database():
    

    def __init__(self,
                 database: str = os.environ.get('POSTGRES_DB'),
                 host: str = os.environ.get('POSTGRES_HOST'),
                 user: str = os.environ.get('POSTGRES_USER'),
                 password: str = os.environ.get('POSTGRES_PASSWORD'),
                 port: str = "5432"):
        """PostgreSQL handler. Implements basic database operations.

        :param database: Database name (default: "postgres").
        :param host: Hostname (default: "pghost").
        :param user: User name (default: "user_name").
        :param password: Password (default: "user_password").
        :param port: Port number. (default: "5432").
        """
        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        
    def connect(self) -> Any:
        """Connect to a database.

        :returns: Database connection handle.
        """
        print(self.host)
        conn = psycopg2.connect(database=self.database,
                                host=self.host,
                                user=self.user,
                                password=self.password,
                                port=self.port)
        # autocommit is not safe practice. Using here for simplicity.
        conn.autocommit = True
        return conn

    def create_tables(self) -> None:
        """Create predefined tables offers, attributes and legacy.
        """
        commands = [
                """
                CREATE TABLE IF NOT EXISTS offers (
                    offerId varchar UNIQUE,
                    PRIMARY KEY (offerId)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS legacy (
                    offerId varchar
                        REFERENCES offers(offerId),
                    platformId varchar,
                    countryCode varchar,
                    platformSellerId varchar,
                    platformOfferId varchar,
                    platformProductId varchar,
                    isOversizeDelivery bool,
                    isDeliveryFeeByQuantity bool,
                    unitWeightGram varchar,
                    isFreeMarketplaceDelivery bool,
                    PRIMARY KEY (offerId)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS attributes (
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
        for cmd in commands:
            try:
                conn = self.connect()
                cur = conn.cursor()
                cur.execute(cmd)
                cur.close()
                conn.commit()
            finally:
                if conn:
                    conn.close()

    def insert_offer(self, offer_id: str) -> None:
        """Insert into offer table.
        
        :param offer_id: Offer ID as PRIMARY KEY.
        """
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
        """Insert into "legacy" table.
                
        :param offer_id: Offer ID as PRIMARY KEY.
        :param legacy_dict: Dictionary with values for columns in table.
        """
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
        """Insert into "attributes" table.
                
        :param offer_id: Offer ID as PRIMARY KEY.
        :param attributes_dict: Dictionary with values for columns in table.
        """
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
