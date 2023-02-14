from datetime import datetime
from pathlib import Path

from common.logger import get_logger
from common.utils import string_to_bytes
from spark_job_processor.events_resolver_base import EventsResolverBase
from spark_job_processor.models import Application, Task

logger = get_logger()


CONFIG_FILE_PATH = Path("configs/events_config.json")


class EventsResolver(EventsResolverBase):
    def __init__(self) -> None:
        super().__init__(path_events_config=CONFIG_FILE_PATH, event_field_name="Event")

    def events_resolver(self, events: list[dict]) -> Application:
        application = Application()
        for event in events:
            match event["Event"]:
                case "SparkListenerApplicationStart":
                    app_start_timestamp = self.find_value_in_event(
                        event, "application_start_time"
                    )
                    application.start_time = datetime.utcfromtimestamp(
                        app_start_timestamp / 1000.0
                    )

                case "SparkListenerApplicationEnd":
                    app_end_timestamp = self.find_value_in_event(
                        event, "application_end_time"
                    )
                    application.end_time = datetime.utcfromtimestamp(
                        app_end_timestamp / 1000.0
                    )

                case "SparkListenerExecutorAdded":
                    executor_start_timestamp = self.find_value_in_event(
                        event, "executor_start_time"
                    )
                    executor_id = self.find_value_in_event(event, "executor_id")
                    _executor = application.executors[executor_id]
                    _executor.start_time = datetime.utcfromtimestamp(
                        executor_start_timestamp / 1000.0
                    )
                    _executor.num_cores = self.find_value_in_event(
                        event, "cores_num"
                    )

                case "SparkListenerTaskEnd":
                    _task_data = {
                        field: self.find_value_in_event(event, field)
                        for field in self.events_config["SparkListenerTaskEnd"]
                    }
                    executor_id = _task_data.pop("executor_id")
                    _task = Task(**_task_data)
                    application.executors[executor_id].tasks.append(_task)

                case "SparkListenerExecutorRemoved" | "SparkListenerExecutorCompleted":
                    executor_id = self.find_value_in_event(event, "executor_id")
                    executor_end_timestamp = self.find_value_in_event(
                        event, "executor_end_time"
                    )
                    application.executors[executor_id][
                        "end_time"
                    ] = datetime.utcfromtimestamp(executor_end_timestamp / 1000.0)

                case "SparkListenerEnvironmentUpdate":
                    try:
                        executor_memory = string_to_bytes(
                            self.find_value_in_event(event, "executor_memory")
                        )
                        memory_overhead_factor = float(
                            self.find_value_in_event(event, "memory_overhead_factor")
                        )
                        application.memory_per_executor = executor_memory * (
                            1 + memory_overhead_factor
                        )
                    except Exception as exc:
                        logger.error(
                            "Failed to parse executor memory from event: %s",
                            exc,
                            exc_info=True,
                        )

        self.post_processing(application)
        return application

    @staticmethod
    def post_processing(application: Application):
        if application.end_time:
            for executor in application.executors.values():
                if executor.end_time is None:
                    executor.end_time = application.end_time
