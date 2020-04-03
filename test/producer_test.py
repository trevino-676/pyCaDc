from py_cadc.producer import ProducerMQ


def test_create_new_producer():
    producer = ProducerMQ('conejo', 'blas', "10.1.8.77", 5672)
    assert producer.channel is not None


def test_send_message():
    producer = ProducerMQ('conejo', 'blas', "10.1.8.77", 5672)
    payload = {"key": "test", "value": "Hola mundo", "canguro": "apache"}
    result = producer.send_message(payload, "test")
    assert result is not False
