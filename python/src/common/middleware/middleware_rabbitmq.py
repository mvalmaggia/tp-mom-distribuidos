import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        self.queue_name = queue_name

    def callback(self, ch, method, properties, body):
        print(f" [x] Received {body.decode()}")
        
    def send(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def start_consuming(self, on_message_callback):

        def pika_callback_wrapper(ch, method, properties, body):
            
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)
            
            on_message_callback(body, ack, nack)

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=pika_callback_wrapper,
            auto_ack=False
        )

        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def close(self):
        self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def send(self, message):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass    

    def close(self):
        pass    