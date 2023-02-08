from unittest import TestCase
import json
from pathlib import Path

from common.models import RawEvent
from spark_job_processor.processor import process_message
from unittest.mock import patch
from datetime import datetime

base_path = Path(__file__).parent


class TestSparkJobProcessor(TestCase):

    @patch('spark_job_processor.processor.insert_metrics_to_db')
    @patch('spark_job_processor.processor.get_events_from_db')
    def test_example_input(self, get_events_from_db_mock, insert_metrics_to_db_mock):
        with open((base_path / 'resources/example_1_input.jsonl').resolve(), 'r') as f:
            events = [json.loads(line) for line in f.readlines()]
            raw_events = []
            for event in events:
                raw_events.append(RawEvent(event=event))

            get_events_from_db_mock.return_value = raw_events

        spark_job_run = process_message(job_run_id='run_id', job_id='job_id')
        spark_job_run = datetime_to_isoformat(spark_job_run)

        with open((base_path / 'resources/example_1_expected_output.json'), 'r') as f:
            expected_output = json.loads(f.read())

        self.assertDictEqual(spark_job_run, expected_output)


def datetime_to_isoformat(dict_to_convert: dict) -> dict:
    for key, value in dict_to_convert.items():
        if isinstance(value, datetime):
            dict_to_convert[key] = value.isoformat()

    return dict_to_convert
