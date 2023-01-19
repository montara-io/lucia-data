import json
from unittest import TestCase
from unittest.mock import patch

from spark_endpoint.app import app, parse_events
from spark_endpoint.config import app_config
from spark_endpoint.models import RawEvent


class TestWriteEvents(TestCase):
    def setUp(self):
        self.app = app
        self.app.config['TESTING'] = True
        self.app.config.from_object(app_config['testing'])
        self.client = self.app.test_client()

    @patch('spark_endpoint.app.write_to_db')
    @patch('spark_endpoint.app.parse_events')
    def test_write_events_ok(self, parse_events_mock, write_to_db_mock):
        parse_events_mock.return_value = (
        [RawEvent(job_run_id='1', event={'Event': 'SparkListenerApplicationEnd'})], True)
        write_to_db_mock.return_value = None
        response = self.client.post('/events', data=json.dumps(
            {'dmAppId': '1', 'data': '{"Event": "SparkListenerApplicationEnd"}'}), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b'OK')

    def test_write_events_missing_param(self):
        response = self.client.post('/events', data=json.dumps({'data': '{"Event": "SparkListenerApplicationEnd"}'}),
                                    content_type='application/json')
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.data, b'Missing dmAppId param (job_run_id), cannot process request')


class TestParseEvents(TestCase):
    def test_parse_events_ok(self):
        unparsed_events = '{"Event": "SparkListenerApplicationStart"}\n{"Event": "SparkListenerApplicationEnd"}'
        job_run_id = '1'
        expected_output = ([RawEvent(job_run_id='1', event={'Event': 'SparkListenerApplicationStart'}),
                            RawEvent(job_run_id='1', event={'Event': 'SparkListenerApplicationEnd'})])
        parsed_events, app_end_event = parse_events(unparsed_events, job_run_id)

        for i, event in enumerate(parsed_events):
            self.assertEqual(event.job_run_id, expected_output[i].job_run_id)
            self.assertEqual(event.event, expected_output[i].event)

        self.assertTrue(app_end_event)

    def test_parse_events_when_no_event(self):
        unparsed_events = ''
        job_run_id = '1'
        expected_output = ([], False)
        result = parse_events(unparsed_events, job_run_id)
        self.assertEqual(result, expected_output)
