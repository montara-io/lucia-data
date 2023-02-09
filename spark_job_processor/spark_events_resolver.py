
import re
import json
from pathlib import Path
from datetime import datetime
from abc import ABC, abstractmethod
from common.logger import get_logger
from spark_job_processor.schemas import SparkApplication, SparkTask

logger = get_logger()


CONFIG_FILE_NAME = "events_config.json"


def read_json(file_path: str):
    with open(file_path, 'r', encoding='utf-8') as json_file:
        return json.load(json_file)


class EventsResolver(ABC):

    def __init__(self, path_events_config: str | Path, event_field_name: str) -> None:
        self.events_config = read_json(path_events_config)
        self.event_field_name = event_field_name

    @abstractmethod
    def events_resolver(self, events: list[dict]):
        """ Resolving the events """

    def find_value_in_event(self, event: str, field_name: str):
        for value in self.events_config[event[self.event_field_name]][field_name]:
            event = event[value]
        return event


class SparkEventsResolver(EventsResolver):

    def __init__(self) -> None:
        super().__init__(
            path_events_config=Path(__file__).with_name(CONFIG_FILE_NAME),
            event_field_name='Event'
        )

    def events_resolver(self, events: list[dict]) -> SparkApplication:
        spark_application = SparkApplication()
        for event in events:
            match event['Event']:
                case 'SparkListenerApplicationStart':
                    app_start_timestamp = self.find_value_in_event(event, 'application_start_time')
                    spark_application.start_time = datetime.fromtimestamp(app_start_timestamp / 1000.0)

                case 'SparkListenerApplicationEnd':
                    app_end_timestamp = self.find_value_in_event(event, 'application_end_time')
                    spark_application.end_time = datetime.fromtimestamp(app_end_timestamp / 1000.0)

                case 'SparkListenerExecutorAdded':
                    executor_start_timestamp = self.find_value_in_event(event, 'executor_start_time')
                    executor_id = self.find_value_in_event(event, 'executor_id')
                    spark_executor = spark_application.executors[executor_id]
                    spark_executor.start_time = datetime.fromtimestamp(executor_start_timestamp / 1000.0)
                    spark_executor.cores_num = self.find_value_in_event(event, 'cores_num')

                case 'SparkListenerTaskEnd':
                    executor_id = self.find_value_in_event(event, 'executor_id')
                    spark_task = SparkTask(**{
                        field: self.find_value_in_event(event, field)
                        for field in self.events_config['SparkListenerTaskEnd']
                    })
                    spark_application.executors[executor_id].tasks.append(spark_task)

                case 'SparkListenerExecutorRemoved' | 'SparkListenerExecutorCompleted':
                    executor_id = self.find_value_in_event(event, 'executor_id')
                    executor_end_timestamp = self.find_value_in_event(event, 'executor_end_time')
                    spark_application.executors[executor_id]['end_time'] = datetime.fromtimestamp(executor_end_timestamp / 1000.0)

                case 'SparkListenerEnvironmentUpdate':
                    try:
                        executor_memory = int(re.search(r'\d+', self.find_value_in_event(event, 'executor_memory')).group())
                        memory_overhead_factor = float(self.find_value_in_event(event, 'memory_overhead_factor'))
                        spark_application.memory_per_executor = (executor_memory * (1 + memory_overhead_factor))
                    except Exception as exc:
                        logger.error("Failed to parse executor memory from event: %s", exc, exc_info=True)

        spark_application.set_totals()
        return spark_application
