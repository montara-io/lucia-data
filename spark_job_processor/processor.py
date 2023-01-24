import re
import os
import math
import json
from datetime import datetime

from spark_job_processor.db_config import DataBaseConfig

global config

executor_info = {
    'cores_num': 0,
    'bytes_read': 0,
    'records_read': 0,
    'bytes_written': 0,
    'records_written': 0,
    'remote_bytes_read': 0,
    'local_bytes_read': 0,
    'shuffle_bytes_read': 0,
    'shuffle_bytes_written': 0,
    'executor_start_time': None,
    'executor_end_time': None,
    'executor_cpu_time': 0,
    'jvm_peak_memory': 0,
    'python_peak_memory': 0,
    'other_peak_memory': 0
}

general_app_info = {
    'id': None,
    'job_id': None,
    'pipeline_id': None,
    'pipeline_run_id': None,
    'start_time': None,
    'end_time': None,
    'num_of_executors': 0,
    'total_memory_per_executor': 0.0,
    'total_cores_num': 0,
    'total_bytes_read': 0,
    'total_bytes_written': 0,
    'total_shuffle_bytes_read': 0,
    'total_shuffle_bytes_written': 0,
    'total_cpu_time_used': 0,
    'total_cpu_uptime': 0,
    'cpu_utilization': 0.0,
    'peak_memory_usage': 0.0
}

id = os.environ.get('job_run_id')
job_id = os.environ.get('job_id')
pipeline_id = os.environ.get('pipeline_id')
pipeline_run_id = os.environ.get('pipeline_run_id')
conn = DataBaseConfig.conn


def get_events_config():
    with open('spark_job_processor/events_config.json') as json_file:
        config_file = json.load(json_file)
    return config_file


def get_events_from_db():
    # with conn.cursor() as cur:
    #     cur.execute("SELECT array_agg(event) FROM raw_event WHERE job_run_id=%s", (id,))
    #     events_data = cur.fetchall()[0][0]
    # return events_data
    with open('input/application_1670830532539_0223_1.txt') as f:
        events = f.readlines()
        events = [json.loads(event) for event in events]
    return events


def find_value_in_event(event, field):
    for value in config[event['Event']][field]:
        event = event[value]
    return event


def collect_relevant_data_from_events(events_list):
    all_executors_info = dict()
    jvm_peak_memory = 0
    python_peak_memory = 0
    other_peak_memory = 0

    for event in events_list:
        match event['Event']:
            case 'SparkListenerApplicationStart':
                app_start_timestamp = find_value_in_event(event, 'application_start_time')
                general_app_info['start_time'] = datetime.fromtimestamp(app_start_timestamp / 1000.0)

            case 'SparkListenerApplicationEnd':
                app_end_timestamp = find_value_in_event(event, 'application_end_time')
                general_app_info['end_time'] = datetime.fromtimestamp(app_end_timestamp / 1000.0)

            case 'SparkListenerExecutorAdded':
                executor_start_timestamp = find_value_in_event(event, 'executor_start_time')
                executor_start_time = datetime.fromtimestamp(executor_start_timestamp / 1000.0)
                executor_id = find_value_in_event(event, 'executor_id')
                all_executors_info[executor_id] = executor_info.copy()
                all_executors_info[executor_id]['cores_num'] = find_value_in_event(event, 'cores_num')
                all_executors_info[executor_id]['executor_start_time'] = executor_start_time

            case 'SparkListenerTaskEnd':
                exc_index = find_value_in_event(event, 'executor_id')
                for field in ['bytes_read', 'records_read', 'bytes_written', 'records_written', 'remote_bytes_read',
                              'local_bytes_read', 'shuffle_bytes_written', 'executor_cpu_time']:
                    all_executors_info[exc_index][field] += find_value_in_event(event, field)

                jvm_peak_memory = max(jvm_peak_memory, find_value_in_event(event, 'jvm_memory'))
                python_peak_memory = max(python_peak_memory, find_value_in_event(event, 'python_memory'))
                other_peak_memory = max(other_peak_memory, find_value_in_event(event, 'other_memory'))
                all_executors_info[exc_index]['jvm_peak_memory'] = jvm_peak_memory
                all_executors_info[exc_index]['python_peak_memory'] = python_peak_memory
                all_executors_info[exc_index]['other_peak_memory'] = other_peak_memory

            case 'SparkListenerExecutorRemoved' | 'SparkListenerExecutorCompleted':
                exc_index = find_value_in_event(event, 'executor_id')
                all_executors_info[exc_index]['executor_end_time'] = datetime.fromtimestamp(
                    find_value_in_event(event, 'executor_end_time') / 1000.0)

            case 'SparkListenerEnvironmentUpdate':
                executor_memory = int(re.search(r'\d+', find_value_in_event(event, 'executor_memory')).group())
                general_app_info['total_memory_per_executor'] = \
                    (executor_memory * (1 + float(find_value_in_event(event, 'memory_overhead_factor'))))

    return general_app_info, all_executors_info


def calc_metrics(general_app_info, all_executors_info):
    max_memory = 0

    for key in all_executors_info:
        all_executors_info[key]['shuffle_bytes_read'] += (all_executors_info[key]['remote_bytes_read'] +
                                                          all_executors_info[key]['local_bytes_read'])

        general_app_info['num_of_executors'] += 1
        for metric in ['cores_num', 'bytes_read', 'bytes_written', 'shuffle_bytes_read', 'shuffle_bytes_written']:
            general_app_info['total_' + metric] += all_executors_info[key][metric]

        general_app_info['total_cpu_time_used'] += (all_executors_info[key]['executor_cpu_time'] / 1e9)

        if all_executors_info[key]['executor_end_time'] is not None:
            all_executors_info[key]['executor_run_time'] = (all_executors_info[key]['executor_end_time'] -
                                                            all_executors_info[key]['executor_start_time'])
        else:
            all_executors_info[key]['executor_run_time'] = (general_app_info['end_time'] -
                                                            all_executors_info[key]['executor_start_time'])

        general_app_info['total_cpu_uptime'] += (all_executors_info[key]['cores_num'] *
                                                 all_executors_info[key]['executor_run_time'].total_seconds())

        executor_memory = (all_executors_info[key]['jvm_peak_memory'] +
                           all_executors_info[key]['python_peak_memory'] +
                           all_executors_info[key]['other_peak_memory'])

        max_memory = max(executor_memory, max_memory)

    general_app_info['cpu_utilization'] = (general_app_info['total_cpu_time_used'] /
                                           general_app_info['total_cpu_uptime']) * 100

    general_app_info['peak_memory_usage'] = (max_memory /
                                             (general_app_info['total_memory_per_executor'] * math.pow(1024, 3))) * 100

    return general_app_info, all_executors_info


def insert_metrics_to_db(general_app_info: dict):
    general_app_info['id'] = id
    general_app_info['job_id'] = job_id
    general_app_info['pipeline_id'] = pipeline_id
    general_app_info['pipeline_run_id'] = pipeline_run_id
    query = "INSERT INTO spark_app_metrics ({}) VALUES ({})"
    columns = ', '.join(general_app_info.keys())
    placeholders = ', '.join(['%s'] * len(general_app_info))
    query = query.format(columns, placeholders)
    with conn.cursor() as cur:
        cur.execute(query, tuple(general_app_info.values()))
        conn.commit()


if __name__ == "__main__":
    config = get_events_config()
    events = get_events_from_db()
    general_app_info, all_executors_info = collect_relevant_data_from_events(events)
    general_app_info, all_executors_info = calc_metrics(general_app_info, all_executors_info)
    # insert_metrics_to_db(general_app_info)
    print(general_app_info)