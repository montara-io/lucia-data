import re
import os
import math
import json
from datetime import datetime

from spark_job_processor.db_config import DataBaseConfig

global config

general_app_info = {
    'job_id': None,
    'job_run_id': None,
    'application_start_time': None,
    'application_end_time': None,
    'num_of_executors': 0,
    'total_memory_per_executor': 0.0,
    'total_application_cores': 0,
    'total_bytes_read': 0,
    'total_bytes_written': 0,
    'total_shuffle_read': 0,
    'total_shuffle_write': 0,
    'total_cpu_time_used': 0,
    'total_cpu_uptime': 0,
    'cpu_usage': 0.0,
    'max_memory_usage': 0.0}

job_id = os.environ.get('job_id')
job_run_id = os.environ.get('job_run_id')
conn = DataBaseConfig.conn


def get_events_config():
    with open('spark_job_processor/events_config.json') as json_file:
        config_file = json.load(json_file)
    return config_file


def get_events_from_db():
    with conn.cursor as cur:
        cur.execute("SELECT array_agg(event) FROM RawEvent WHERE job_run_id=%s", job_run_id)
        events_data = cur.fetchall()[0][0]
    return events_data


def find_value_in_event(event, field):
    for value in config[event['Event']][field]:
        event = event[value]
    return event


def collect_relevant_data_from_events(events_list):
    executors_info = dict()
    jvm_peak_memory = 0
    python_peak_memory = 0
    other_peak_memory = 0

    for event in events_list:
        match event['Event']:
            case 'SparkListenerApplicationStart':
                app_start_timestamp = find_value_in_event(event, 'application_start_time')
                general_app_info['application_start_time'] = datetime.fromtimestamp(app_start_timestamp / 1000.0)

            case 'SparkListenerApplicationEnd':
                app_end_timestamp = find_value_in_event(event, 'application_end_time')
                general_app_info['application_end_time'] = datetime.fromtimestamp(app_end_timestamp / 1000.0)

            case 'SparkListenerExecutorAdded':
                executor_start_timestamp = find_value_in_event(event, 'executor_start_time')
                executor_start_time = datetime.fromtimestamp(executor_start_timestamp / 1000.0)
                total_cores = find_value_in_event(event, 'total_cores')
                executors_info[find_value_in_event(event, 'executor_id')] = \
                    {'Total Cores': total_cores, 'Bytes Read': 0, 'Records Read': 0,
                     'Bytes Written': 0, 'Records Written': 0, 'Shuffle Read': 0, 'Shuffle Write': 0,
                     'Executor Start Time': executor_start_time, 'Executor End Time': None,
                     'Executor CPU Time': 0, 'JVM Peak Memory': 0, 'Python Peak Memory': 0, 'Other Peak Memory': 0}

            case 'SparkListenerTaskEnd':
                exc_index = find_value_in_event(event, 'executor_id')
                executors_info[exc_index]['Bytes Read'] += find_value_in_event(event, 'bytes_read')
                executors_info[exc_index]['Records Read'] += find_value_in_event(event, 'records_read')
                executors_info[exc_index]['Bytes Written'] += find_value_in_event(event, 'bytes_written')
                executors_info[exc_index]['Records Written'] += find_value_in_event(event, 'records_written')
                executors_info[exc_index]['Shuffle Read'] += (find_value_in_event(event, 'remote_bytes_read') +
                                                              find_value_in_event(event, 'local_bytes_read'))
                executors_info[exc_index]['Shuffle Write'] += find_value_in_event(event, 'shuffle_bytes_written')
                executors_info[exc_index]['Executor CPU Time'] += find_value_in_event(event, 'executor_cpu_time')
                jvm_peak_memory = max(jvm_peak_memory, find_value_in_event(event, 'jvm_memory'))
                python_peak_memory = max(python_peak_memory, find_value_in_event(event, 'python_memory'))
                other_peak_memory = max(other_peak_memory, find_value_in_event(event, 'other_memory'))
                executors_info[exc_index]['JVM Peak Memory'] = jvm_peak_memory
                executors_info[exc_index]['Python Peak Memory'] = python_peak_memory
                executors_info[exc_index]['Other Peak Memory'] = other_peak_memory

            case 'SparkListenerExecutorRemoved' | 'SparkListenerExecutorCompleted':
                exc_index = find_value_in_event(event, 'executor_id')
                executors_info[exc_index]['Executor End Time'] = datetime.fromtimestamp(
                    find_value_in_event(event, 'executor_end_time') / 1000.0)

            case 'SparkListenerEnvironmentUpdate':
                executor_memory = int(re.search(r'\d+', find_value_in_event(event, 'executor_memory')).group())
                general_app_info['total_memory_per_executor'] = \
                    (executor_memory * (1 + float(find_value_in_event(event, 'memory_overhead_factor'))))

    return general_app_info, executors_info


def calc_metrics(general_app_info, executors_info):
    max_memory = 0

    for key in executors_info:
        general_app_info['num_of_executors'] += 1
        general_app_info['total_application_cores'] += executors_info[key]['Total Cores']
        general_app_info['total_bytes_read'] += executors_info[key]['Bytes Read']
        general_app_info['total_bytes_written'] += executors_info[key]['Bytes Written']
        general_app_info['total_shuffle_read'] += executors_info[key]['Shuffle Read']
        general_app_info['total_shuffle_write'] += executors_info[key]['Shuffle Write']
        general_app_info['total_cpu_time_used'] += (executors_info[key]['Executor CPU Time'] / 1e9)
        if executors_info[key]['Executor End Time'] is not None:
            executors_info[key]['Executor Run time'] = (executors_info[key]['Executor End Time'] -
                                                        executors_info[key]['Executor Start Time'])
        else:
            executors_info[key]['Executor Run time'] = (general_app_info['application_end_time'] -
                                                        executors_info[key]['Executor Start Time'])

        general_app_info['total_cpu_uptime'] += (executors_info[key]['Total Cores'] *
                                                 executors_info[key]['Executor Run time'].total_seconds())

        executor_memory = (executors_info[key]['JVM Peak Memory'] +
                           executors_info[key]['Python Peak Memory'] +
                           executors_info[key]['Other Peak Memory'])

        max_memory = max(executor_memory, max_memory)

    general_app_info['cpu_usage'] = (general_app_info['total_cpu_time_used'] /
                                     general_app_info['total_cpu_uptime']) * 100

    general_app_info['max_memory_usage'] = (max_memory /
                                            (general_app_info['total_memory_per_executor'] * math.pow(1024, 3))) * 100

    return general_app_info, executors_info


def insert_metrics_to_db(general_app_info: dict):
    general_app_info['job_id'] = job_id
    general_app_info['job_run_id'] = job_run_id
    query = "INSERT INTO SparkAppMetrics ({}) VALUES ({})"
    columns = ', '.join(general_app_info.keys())
    placeholders = ', '.join(['%s'] * len(general_app_info))
    query = query.format(columns, placeholders)
    with conn.cursor as cur:
        cur.execute(query, tuple(general_app_info.values()))
        conn.commit()


if __name__ == "__main__":
    config = get_events_config()
    events = get_events_from_db()
    general_app_info, executors_info = collect_relevant_data_from_events(events)
    general_app_info, executors_info = calc_metrics(general_app_info, executors_info)
    insert_metrics_to_db(general_app_info)
