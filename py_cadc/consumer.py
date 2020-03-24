import threading

import pika


class ConsumerMQ(threading.Thread):
    """
    This class contains the implementation of a rabbitmq consumer
    """
    def __init__(
        self, host, port, queue, group=None, 
        target=None, name=None, args=(), kwargs=None, daemon=None
    ):
        super().__init__(group=group, target=target, name=name, daemon=daemon)
        try:
            self.__connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host, port=port)
            )
            self.channel = self.__connection.channel()
        except Exception as err:
            print(err)
            self.channel = None

    def run(self):
        """
        This method contains the implementation of what the
        thread will execute when called
        """
        self.channel.basic_consume(
            queue="test", auto_ack=True, on_message_callback=self.callback
        )
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        print(f"    [x] {body}")