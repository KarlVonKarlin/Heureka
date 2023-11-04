from resources import connect
 
connection, channel = connect()
 
def on_receive(ch, method, properties, body):
    print("Received %r" % body)
 
 
channel.basic_consume(on_message_callback=on_receive,
                      queue='test',
                      auto_ack=True)
 
print('Waiting for messages. To exit press CTRL+C')
print("...")
channel.start_consuming()

# class Messenger():

# class Parser ():
    
#     __init__(msg_json: dict):
#     """_summary_
#     """
#     msg_json = msg_json