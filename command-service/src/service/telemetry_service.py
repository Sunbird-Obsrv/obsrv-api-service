import json
import os

from kafka import KafkaProducer

from config import Config
from model.telemetry_models import Actor, Audit, Context, Object, Pdata, Telemetry


class TelemetryService:
    def __init__(self) -> None:
        self.actor = Actor()
        self.pdata = Pdata("{}.command.service".format(os.getenv("system_env", "dev")))
        self.context = Context(self.pdata, os.getenv("system_env", "dev"))
        self.config_obj = Config()
        try:
            self.producer = KafkaProducer(
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                bootstrap_servers=self.config_obj.find("kafka.brokers"),
            )
        except Exception as e:
            print("Error while connecting to kafka - ", e)
            self.producer = None
        self.topic = (
            os.getenv("system_env", "dev")
            + "."
            + self.config_obj.find("kafka.telemetry.topic")
        )

    def audit(self, object_: Object, edata: Audit):
        event = Telemetry(
            actor=self.actor, context=self.context, object=object_, edata=edata
        )
        data = event.to_json()
        print("Audit event - ", json.dumps(data, indent=4))

        if self.producer:
            self.producer.send(self.topic, data)
