import pika

from producer import ProducerMQ


def main():
    producer = ProducerMQ(host="10.1.8.32", port=5672)
    print(producer.channel)


def consume_message():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="10.1.8.32", port=5672)
    )
    channel = connection.channel()
    channel.basic_consume(queue="test", auto_ack=True, on_message_callback=callback)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def callback(ch, method, properties, body):
    print(f"[x] Received {body}")


if __name__ == "__main__":
    consume_message()