from datetime import datetime, timedelta, timezone
from unittest import TestCase

from common.utils import read_json
from spark_job_processor.events_resolver import (
    CONFIG_FILE_PATH,
    EventsResolver,
)
from spark_job_processor.models import Application, Executor, Task


class TestEventsResolver(TestCase):
    def test_events_resolver_events_config(self):
        events_resolver = EventsResolver()
        assert events_resolver.events_config == read_json(CONFIG_FILE_PATH)

    def test_events_resolver(self):
        start_datetime = datetime(2023, 1, 1, 6, 0, 0)
        end_datetime = datetime(2023, 1, 1, 7, 0, 0)
        events = [
            {
                "Event": "SparkListenerApplicationStart",
                "Timestamp": start_datetime.replace(tzinfo=timezone.utc).timestamp() * 1000,
            },
            {
                "Event": "SparkListenerExecutorAdded",
                "Timestamp": (start_datetime + timedelta(minutes=10)).replace(tzinfo=timezone.utc).timestamp()
                * 1000,
                "Executor ID": "1",
                "Executor Info": {"Total Cores": 4},
            },
            {
                "Event": "SparkListenerTaskEnd",
                "Task Info": {"Executor ID": "1"},
                "Task Executor Metrics": {
                    "ProcessTreeJVMRSSMemory": 2172276736,
                    "ProcessTreePythonRSSMemory": 0,
                    "ProcessTreeOtherRSSMemory": 0,
                },
                "Task Metrics": {
                    "Executor CPU Time": 1618960992,
                    "Shuffle Read Metrics": {
                        "Remote Bytes Read": 0,
                        "Local Bytes Read": 0,
                    },
                    "Shuffle Write Metrics": {"Shuffle Bytes Written": 1258},
                    "Input Metrics": {"Bytes Read": 8707238, "Records Read": 12815},
                    "Output Metrics": {"Bytes Written": 0, "Records Written": 0},
                },
            },
            {
                "Event": "SparkListenerEnvironmentUpdate",
                "Spark Properties": {
                    "spark.executor.memory": "18971M",
                    "spark.yarn.executor.memoryOverheadFactor": "0.1875",
                },
            },
            {
                "Event": "SparkListenerApplicationEnd",
                "Timestamp": end_datetime.replace(tzinfo=timezone.utc).timestamp() * 1000,
            },
        ]
        events_resolver = EventsResolver()
        actual_application = events_resolver.events_resolver(events)

        expected_application = Application(
            start_time=start_datetime,
            end_time=end_datetime,
            memory_per_executor=23622385664.0,
        )
        expected_application.executors["1"] = Executor(
            start_time=start_datetime + timedelta(minutes=10),
            end_time=end_datetime,
            num_cores=4,
            tasks=[
                Task(
                    cpu_time=1618960992,
                    bytes_read=8707238,
                    records_read=12815,
                    bytes_written=0,
                    records_written=0,
                    shuffle_remote_bytes_read=0,
                    shuffle_local_bytes_read=0,
                    shuffle_bytes_read=0,
                    shuffle_bytes_written=1258,
                    jvm_memory=2172276736,
                    python_memory=0,
                    other_memory=0,
                )
            ],
        )
        self.assertEqual(actual_application, expected_application)
