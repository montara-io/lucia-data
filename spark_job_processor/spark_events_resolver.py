from datetime import datetime
from pathlib import Path

from common.logger import get_logger
from common.utils import string_to_bytes
from spark_job_processor.events_resolver import EventsResolver
from spark_job_processor.spark_schemas import SparkApplication, SparkTask

logger = get_logger()


CONFIG_FILE_PATH = Path("configs/spark_events_config.json")


class SparkEventsResolver(EventsResolver):
    def __init__(self) -> None:
        super().__init__(path_events_config=CONFIG_FILE_PATH, event_field_name="Event")

    def events_resolver(self, events: list[dict]) -> SparkApplication:
        spark_application = SparkApplication()
        for event in events:
            match event["Event"]:
                case "SparkListenerApplicationStart":
                    app_start_timestamp = self.find_value_in_event(
                        event, "application_start_time"
                    )
                    spark_application.start_time = datetime.fromtimestamp(
                        app_start_timestamp / 1000.0
                    )

                case "SparkListenerApplicationEnd":
                    app_end_timestamp = self.find_value_in_event(
                        event, "application_end_time"
                    )
                    spark_application.end_time = datetime.fromtimestamp(
                        app_end_timestamp / 1000.0
                    )

                case "SparkListenerExecutorAdded":
                    executor_start_timestamp = self.find_value_in_event(
                        event, "executor_start_time"
                    )
                    executor_id = self.find_value_in_event(event, "executor_id")
                    spark_executor = spark_application.executors[executor_id]
                    spark_executor.start_time = datetime.fromtimestamp(
                        executor_start_timestamp / 1000.0
                    )
                    spark_executor.num_cores = self.find_value_in_event(
                        event, "cores_num"
                    )

                case "SparkListenerTaskEnd":
                    executor_id = self.find_value_in_event(event, "executor_id")
                    spark_task = SparkTask(
                        **{
                            field: self.find_value_in_event(event, field)
                            for field in self.events_config["SparkListenerTaskEnd"]
                        }
                    )
                    spark_application.executors[executor_id].tasks.append(spark_task)

                case "SparkListenerExecutorRemoved" | "SparkListenerExecutorCompleted":
                    executor_id = self.find_value_in_event(event, "executor_id")
                    executor_end_timestamp = self.find_value_in_event(
                        event, "executor_end_time"
                    )
                    spark_application.executors[executor_id][
                        "end_time"
                    ] = datetime.fromtimestamp(executor_end_timestamp / 1000.0)

                case "SparkListenerEnvironmentUpdate":
                    try:
                        executor_memory = string_to_bytes(
                            self.find_value_in_event(event, "executor_memory")
                        )
                        memory_overhead_factor = float(
                            string_to_bytes(
                                self.find_value_in_event(
                                    event, "memory_overhead_factor"
                                )
                            )
                        )
                        spark_application.memory_per_executor = executor_memory * (
                            1 + memory_overhead_factor
                        )
                    except Exception as exc:
                        logger.error(
                            "Failed to parse executor memory from event: %s",
                            exc,
                            exc_info=True,
                        )

        self.post_processing(spark_application)
        return spark_application

    @staticmethod
    def post_processing(spark_application: SparkApplication):
        if spark_application.end_time:
            for executor in spark_application.executors.values():
                if executor.end_time is None:
                    executor.end_time = spark_application.end_time
