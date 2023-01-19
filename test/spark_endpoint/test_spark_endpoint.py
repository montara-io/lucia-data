import os
from unittest import TestCase
from spark_endpoint.app import parse_events, app
import json


class TestHome(TestCase):
    def test_write_events(self):
        with app.test_client() as c:
            response = c.post('/events', data=json.dumps({"dmAppId": "some_id", "data": "{\"Event\":\"SparkListenerLogStart\",\"Spark Version\":\"3.3.1\"}\n"}), content_type='application/json')
            self.assertEqual(response.status_code, 200)

    def test_parse_events(self):
        parsed_events, _ = parse_events(
            unparsed_events="{\"Event\":\"Event1\",\"Spark Version\":\"3.3.1\"}\n{\"Event\":\"Event2\",\"Spark Version\":\"3.3.1\"}\n",
            job_run_id="some_id")
        self.assertEqual(parsed_events[0].job_run_id, "some_id")
        self.assertEqual(parsed_events[0].event, {"Event": "Event1", "Spark Version": "3.3.1"})
        self.assertEqual(parsed_events[1].job_run_id, "some_id")
        self.assertEqual(parsed_events[1].event, {"Event": "Event2", "Spark Version": "3.3.1"})

    def test_parse_events_app_end(self):
        _, app_end_event = parse_events(
            unparsed_events="{\"Event\":\"SparkListenerApplicationEnd\",\"Spark Version\":\"3.3.1\"}\n",
            job_run_id="some_id")
        self.assertEqual(app_end_event, True)

    def test_missing_job_run_id(self):
        with app.test_client() as c:
            response = c.post('/events', data=json.dumps({"data": ""}), content_type='application/json')
            self.assertEqual(response.status_code, 400)