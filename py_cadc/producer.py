# import json
# from datetime import datetime

import pika


class ProducerMQ():
    """
    This class contains the methods to post messages in a message queue with rabbitmq
    """

    def __init__(self, host="localhost", port=5672):
        """
        Constructor of the ProducerMQ class

        Parameters:
            host: hostname or ip address of the rabbitmq server <str>
            port: port number of the rabbitmq server <int>
        """
        try:
            self.__connection = pika.BlockingConnection(
                pika.ConnectionParameters(host, port)
            )
            self.channel = self.__connection.channel()
        except Exception as err:
            print(err)
            self.__connection = None
            self.channel = None
  
    def send_message(self, message, routing_key, queue="test"):
        """
        This method send a message to queue from rabbitMQ server declared in the object

        :param message:  message body
        :param routing_key: The routing key to bind on
        :param queue: name of queue in rabbitmq server
        :type message: string
        :type routing_key: string
        :type queue: string
        :return: True if the message was sent correctly else False
        :rtype: boolean
        """
        band = False
        try:
            self.channel.queue_declare(queue=queue, durable=True)
            self.channel.basic_publish(
                exchange="", routing_key=routing_key, body=message
            )
            band = True
        except Exception as err:
            print(err)
        finally:
            self.channel.close()
            self.__connection.close()
            return band
