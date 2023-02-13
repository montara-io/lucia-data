import json
from datetime import datetime
from pathlib import Path
from unittest import TestCase

from spark_job_processor.spark_events_processor import SparkEventsProcessor

base_path = Path(__file__).parent


class TestSparkEventsProcessor(TestCase):

    def test_example_input(self):
        with open((base_path / 'resources/example_1_input.jsonl').resolve(), 'r', encoding='utf-8') as file:
            events = [json.loads(line) for line in file.readlines()]

        application_data = SparkEventsProcessor().process_events(events, job_run_id='run_id', job_id='job_id')

        with open((base_path / 'resources/example_1_expected_output.json'), 'r', encoding='utf-8') as file:
            expected_output = json.loads(file.read())
            expected_output = iso_format_to_datetime(expected_output)

        self.assertDictEqual(application_data, expected_output)


def iso_format_to_datetime(dict_to_convert: dict) -> dict:
    for key, value in dict_to_convert.items():
        if key.endswith('_time'):
            dict_to_convert[key] = datetime.fromisoformat(value)

    return dict_to_convert
