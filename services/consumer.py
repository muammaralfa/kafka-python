import json
from datetime import datetime
from kafka import KafkaConsumer
from services.producer import Producer
from config.base import app_setting
from loguru import logger


class Consumer:
    def __init__(self):
        self.topic: str = app_setting.TOPIC
        self.bootstrap_servers: str = app_setting.BOOTSTRAP_SERVER
        self.group_id: str = app_setting.GROUP_ID
        self.auto_offset_reset: str = app_setting.AUTO_OFFSET_RESET
        self.producer = Producer()

    def start_consume(self) -> KafkaConsumer:
        """
        initialize kafka consumer then return it

        :return:
        """
        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers.split(","),
            auto_offset_reset=self.auto_offset_reset,
            group_id=self.group_id,
            enable_auto_commit=True,
            max_poll_records=1000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        consumer.subscribe(self.topic.split(","))
        print("consume starting...")
        return consumer

    def consume(self):
        """
        main function of consumer

        :return:
        """
        consumer = self.start_consume()
        try:
            while True:
                messages = consumer.poll(1.0)
                for _, message_list in messages.items():
                    print(f">>>>> consuming {len(message_list)} messages")
                    for message in message_list:
                        data = message.value
                        topic = message.topic
                        self.producer.produce(message=data)

        except Exception as e:
            self.producer.close()
            raise e
