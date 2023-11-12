import pytest
import sys
from pathlib import Path
import psycopg2
import logging

LOG_FORMAT = '%(asctime)s;%(levelname)s;%(message)s'
LOG = logging.getLogger(__name__)



class TestDb():

    @staticmethod
    @pytest.fixture(scope='class')
    def db() -> Database:
        """Database module tes.
        """
        try:
            db = Database(database = "testing-db")
        except Exception as err:
            LOG.error(err)
        finally:
            return db
                
    def test_create_tables(self, db) -> None:
        

        conn = db.connect()
        cur = conn.cursor()
        cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                    """)
        if cur.fetchall():
            cur.execute("DROP SCHEMA public CASCADE")
            cur.execute("CREATE SCHEMA public;")
        cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                    """)
        if cur.fetchall():
            pytest.fail('Drop and recreate schema failed!')
        
        db.create_tables()
        cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                    """)
        tables = cur.fetchall()
        if ('offers',) not in tables or \
           ('legacy',) not in tables or \
           ('attributes',) not in tables:
            pytest.fail(f'Failed to create tables!')
        
        cur.close()
        conn.close()
    
    def test_offer(self, db) -> None:
        
        conn = db.connect()
        cur = conn.cursor()
        
        offer_id = 'offer-id-1'
        cur.execute('SELECT offerId FROM offers WHERE offerId = %s', (offer_id,))
        if cur.fetchone() and ('offer-id-1',) in cur.fetchone():
            pytest.fail(f'Id "{offer_id}" already present in table "offers"')
        db.insert_offer(offer_id)
        
        cur.execute('SELECT offerId FROM offers WHERE offerId = %s', (offer_id,))
        if 'offer-id-1' not in cur.fetchone():
            pytest.fail(f'Failed to insert id "{offer_id}" into table "offers"')

    def test_insert_legacy(self, db) -> None:
        
        conn = db.connect()
        cur = conn.cursor()
        offer_id = 'offer-id-1'
        attr_dict = {
          "platformId": "heu",
          "countryCode": "CZ",
          "platformSellerId": "289",
          "platformOfferId": "4875842584",
          "platformProductId": "982035159",
          "isOversizeDelivery": True,
          "isDeliveryFeeByQuantity": True,
          "unitWeightGram": "unit",
          "isFreeMarketplaceDelivery": True
        }
        
        cur.execute(
            """
            SELECT (
                offerId,
                platformId,
                platformSellerId,
                platformOfferId,
                platformProductId,
                isOversizeDelivery,
                isDeliveryFeeByQuantity,
                unitWeightGram,
                isFreeMarketplaceDelivery
                )
                FROM legacy
                WHERE offerId = %s
            """,
            (offer_id,))
        if cur.fetchone() and ('offer-id-1',) in cur.fetchone():
            pytest.fail(f'Id "{offer_id}" already present in table "legacy"')
        
        db.insert_offer(offer_id)
        
        db.insert_legacy(id, attr_dict)
        