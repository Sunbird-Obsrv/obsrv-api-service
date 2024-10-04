from command.icommand import ICommand
from command.icommand import ICommand
from config import Config
from model.data_models import Action, ActionResponse, CommandPayload
from service.http_service import HttpService
from confluent_kafka.admin import AdminClient, NewTopic
from command.dataset_command import DatasetCommand

class KafkaCommand(ICommand):
    def __init__(self, config: Config, http_service: HttpService, dataset_command: DatasetCommand):
        self.config = config
        self.http_service = http_service
        self.dataset_command = dataset_command
        
    def execute(self, command_payload: CommandPayload, action: Action):
        result = None
        if action == Action.CREATE_KAFKA_TOPIC.name:
            print(
                f"Invoking CREATE_KAFKA_TOPIC command for dataset_id {command_payload.dataset_id}..."
            )
            self.config_obj = Config()
            dataset_id = command_payload.dataset_id
            live_dataset, data_version = self.dataset_command._check_for_live_record(
                dataset_id
            )
            topic = live_dataset.router_config['topic']
            brokers = self.config_obj.find("kafka.brokers")
            print(f"broker", brokers)
            result = self.create_kafka_topic(topic, brokers, 1, 1)
        return result


    def create_kafka_topic(self, topic, broker, num_partitions, replication_factor):
        admin_client = AdminClient({'bootstrap.servers': broker})
        print(f"topic is",topic)
        new_topic = [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)]

        try:
            fs = admin_client.create_topics(new_topic)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Topic '{topic}' created successfully")
                except Exception as e:
                    print(f"Failed to create topic '{topic}': {e}")
        except Exception as e:
            print(f"Error creating topic: {e}")  
        
        return ActionResponse(status="OK", status_code=200)     