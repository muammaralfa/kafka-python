import json
from config.base import app_setting
from kafka import KafkaProducer


class Producer:
    def __init__(self):
        self.bootstrap_server = app_setting.BOOTSTRAP_SERVER
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_server.split(','),
            value_serializer=lambda messages: json.dumps(messages).encode('utf-8')
        )
        self.topic = app_setting.TOPIC_RESULT_DEMOGRAPHY

    def produce(self, message):
        """
        just send message into kafka

        :param message:
        :return:
        """
        self.producer.send(self.topic, message)
        self.producer.flush()

    def close(self):
        self.producer.close()