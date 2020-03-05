from py_cadc.producer import ProducerMQ


def test_create_new_producer():
    producer = ProducerMQ("10.1.8.32", 5672)
    assert producer.channel is not None


def test_send_message():
    producer = ProducerMQ("10.1.8.32", 5672)
    result = producer.send_message("test message", "test")
    assert result is not False