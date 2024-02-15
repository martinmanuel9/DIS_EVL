import asyncio
import json
import logging
import os
import struct
from collections import namedtuple

from threading import Thread

import certifi
from confluent_kafka import Producer, KafkaException
from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka.serialization import (
    Deserializer, Serializer, SerializationError
)

Consumer = KafkaConsumer

logger = logging.getLogger()


KafkaMessage = namedtuple("KafkaMessage", ["key", "value"])


TLS_DEFAULTS = {
    "security.protocol": "ssl",
    "ssl.ca.location": certifi.where()
}


def default_config(config=None, secure=True):
    new_config = {}
    if os.environ.get("KAFKA_TLS_BROKERS", "") != "":
        new_config.update(TLS_DEFAULTS)
        new_config["bootstrap.servers"] = os.environ["KAFKA_TLS_BROKERS"]
    else:
        new_config["bootstrap.servers"] = "172.18.0.4:9092"
    new_config.update(config or {})
    return new_config


class AsyncKafkaProducer(object):
    def __init__(self, config, **kwargs):
        self.loop = asyncio.get_event_loop()
        self.producer = Producer(config, **kwargs)
        self.cancelled = False
        self.poller = Thread(target=self.poll)
        self.poller.start()

    def poll(self):
        logger.info("Starting polling")
        while not self.cancelled:
            self.producer.poll(0.1)
        logger.info("Stopping polling - cancelled %s", self.cancelled)

    def close(self):
        self.cancelled = True

    def produce(self, topic, key, value):
        result = self.loop.create_future()

        def ack(err, msg):
            if err:
                self.loop.call_soon_threadsafe(
                    result.set_exception,
                    KafkaException(err))
            else:
                self.loop.call_soon_threadsafe(
                    result.set_result,
                    msg)
        logger.debug("Producing message to %s with key %s", topic, key)
        self.producer.produce(topic, key=key, value=value, on_delivery=ack)
        return result


async def consume(consumer, topic):
    consumer.subscribe([topic])
    logger.debug("Subscribed consumer to topic %s", topic)
    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
                await asyncio.sleep(.01)
                continue
            if msg.error():
                yield None, msg.error()
            else:
                yield msg, None
            consumer.commit(msg)
    except RuntimeError:
        raise StopIteration
