import os
import sys
from typing import Callable

import pika


class Rabbit:
    host: str = "rabbit"
    port: int = 5672
    
    def connect(self):
        """
        Connect to the RabbitMQ service.
        """
        
        connection_params = pika.ConnectionParameters(Rabbit.host, Rabbit.port)
        return pika.BlockingConnection(connection_params)
    

class Producer(Rabbit):
    """
    Produce messages to send to queue.
    """
    
    def send(self, queue_name: str, body: str):
        try:
            connection = self.connect()
            channel = connection.channel()
            channel.queue_declare(queue=queue_name)
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=body
            )
        except Exception as e:
            raise e
        finally:
            connection.close()
    

class Consumer(Rabbit):
    """
    Consume messages from queue.
    """
    
    def receive(self, queue_name: str, callback: Callable):
        connection = self.connect()
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)
        channel.basic_consume(queue=queue_name, on_message_callback=callback)

        try:
            print("Receiving")
            channel.start_consuming()
        except KeyboardInterrupt:
            print('Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)