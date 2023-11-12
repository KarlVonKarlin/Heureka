import sys
import unittest
import unittest.mock
from pathlib import Path
import pika

sys.path.append(str(Path(__file__).parents[1]))
from heureka.src.producer.producer import Producer

class TestConsumer(unittest.TestCase):
    
    producer = Producer()
    mock_file_content = """
        {
            "offers":[
                {
                    "id": "917d18b1-0433-5fe4-99e5-ed92f7050b87",
                    "version": 1696840352,
                    "sellerOfferId": "OS-MARINAP-00"
                }
            ]
        }
        """
    
    def test_producer_prepare_msg_from_file(self):
        
        with unittest.mock.patch(
            'builtins.open',
            new=unittest.mock.mock_open(read_data=self.mock_file_content),
            create=True
        ) as file_mock:
            self.assertEqual(
                self.producer.prepare_msg_from_file('/dev/null'),
                b'{"offers": [{"id": "917d18b1-0433-5fe4-99e5-ed92f7050b87", '
                b'"version": 1696840352, "sellerOfferId": "OS-MARINAP-00"}]}'
            )

    # @unittest.mock.patch('pika.BlockingConnection', spec=pika.BlockingConnection)  
    # def test_publish(self, mocked_connection):
        
    #     with self.producer as producer:
    #        mocked_connection.return_value.channel.return_value.basic_publish.return_value = False


    # def test_close_connection(self):
        
        
        
if __name__ == '__main__':
    # begin the unittest.main()
    unittest.main()