import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError, MessageMiddlewareMessageError


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        self.queue_name = queue_name

    def callback(self, ch, method, properties, body):
        print(f" [x] Received {body.decode()}")
        
    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError("Connection lost while sending message.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while sending message: {str(e)}")


    def start_consuming(self, on_message_callback):
        def pika_callback_wrapper(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)
            
            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=pika_callback_wrapper,
                auto_ack=False
            )

            self.channel.start_consuming()
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError("Connection lost while consuming messages.")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"An error occurred while consuming messages: {str(e)}")

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except (AMQPConnectionError, AMQPChannelError):             
            raise MessageMiddlewareDisconnectedError("Connection lost while stopping consumption.")
        except Exception:
            pass

    def close(self):
        try:
            if self.connection.is_open:
                self.connection.close()
        except:
            raise MessageMiddlewareCloseError("Failed to close the connection properly.")

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self._exhange_name = exchange_name
        self._routing_keys = routing_keys
        self._bind_to_queues(routing_keys)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    def _bind_to_queues(self, routing_keys):
        for routing_key in routing_keys:
            queue_name = f"{self._exhange_name}_{routing_key}"
            self.channel.queue_declare(queue=queue_name)
            self.channel.queue_bind(exchange=self._exhange_name, queue=queue_name, routing_key=routing_key)

    def send(self, message):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass    

    def close(self):
        pass    