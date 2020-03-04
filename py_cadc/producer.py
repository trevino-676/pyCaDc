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
                pika.ConnectionParameters(host=host, port=port)
            )
            self.__channel = self.__connection.channel()
        except Exception as err:
            print(err.with_traceback())
            self.__connection = None
            self.__channel = None
