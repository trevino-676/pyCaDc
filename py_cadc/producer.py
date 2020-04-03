import json

import pika


class ProducerMQ:
    """
    This class contains the methods to post messages in a message queue with rabbitmq
    """

    def __init__(self, user: str, passwd: str, host="localhost", port=5672):
        """
        Constructor of the ProducerMQ class

        Parameters:
            host: hostname or ip address of the rabbitmq server <str>
            port: port number of the rabbitmq server <int>
        """
        credentials = pika.PlainCredentials(user, passwd)
        try:
            self.__connection = pika.BlockingConnection(
                pika.ConnectionParameters(host, port, credentials=credentials)
            )
            self.channel = self.__connection.channel()
        except Exception as err:
            print(err)
            self.__connection = None
            self.channel = None

    def send_message(self, payload, routing_key, queue="test"):
        """
        This method send a message to queue from rabbitMQ server declared in the object

        :param payload:  payload body
        :param routing_key: The routing key to bind on
        :param queue: name of queue in rabbitmq server
        :type message: dict
        :type routing_key: string
        :type queue: string
        :return: True if the message was sent correctly else False
        :rtype: boolean
        """
        band = False
        try:
            self.channel.queue_declare(queue=queue, durable=True)
            self.channel.basic_publish(
                exchange="",
                routing_key=routing_key,
                body=json.dumps(payload, sort_keys=True),
            )
            band = True
        except Exception as err:
            print(err)
        finally:
            self.channel.close()
            self.__connection.close()
            return band
