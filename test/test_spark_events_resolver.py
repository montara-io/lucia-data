from datetime import datetime, timedelta
from unittest import TestCase

from common.utils import read_json
from spark_job_processor.spark_events_resolver import (
    CONFIG_FILE_PATH,
    SparkEventsResolver,
)
from spark_job_processor.spark_schemas import SparkApplication, SparkExecutor, SparkTask


class TestSparkEventsResolver(TestCase):
    def test_spark_events_resolver_events_config(self):
        spark_events_resolver = SparkEventsResolver()
        assert spark_events_resolver.events_config == read_json(CONFIG_FILE_PATH)

    def test_spark_events_resolver(self):
        start_datetime = datetime(2023, 1, 1, 6, 0, 0)
        end_datetime = datetime(2023, 1, 1, 7, 0, 0)
        events = [
            {
                "Event": "SparkListenerApplicationStart",
                "Timestamp": start_datetime.timestamp() * 1000,
            },
            {
                "Event": "SparkListenerExecutorAdded",
                "Timestamp": (start_datetime + timedelta(minutes=10)).timestamp()
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
                "Timestamp": end_datetime.timestamp() * 1000,
            },
        ]
        spark_events_resolver = SparkEventsResolver()
        actual_spark_application = spark_events_resolver.events_resolver(events)

        expected_spark_application = SparkApplication(
            start_time=start_datetime,
            end_time=end_datetime,
            memory_per_executor=0,
        )
        expected_spark_application.executors["1"] = SparkExecutor(
            start_time=start_datetime + timedelta(minutes=10),
            end_time=end_datetime,
            num_cores=4,
            tasks=[
                SparkTask(
                    cpu_time=1618960992,
                    bytes_read=8707238,
                    records_read=12815,
                    bytes_written=0,
                    records_written=0,
                    remote_bytes_read=0,
                    local_bytes_read=0,
                    shuffle_bytes_read=0,
                    shuffle_bytes_written=1258,
                    jvm_memory=2172276736,
                    python_memory=0,
                    other_memory=0,
                )
            ],
        )
        self.assertEqual(actual_spark_application, expected_spark_application)
