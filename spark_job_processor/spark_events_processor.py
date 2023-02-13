from spark_job_processor.spark_events_resolver import SparkEventsResolver


class SparkEventsProcessor:
    def __init__(self) -> None:
        self.resolver: SparkEventsResolver = SparkEventsResolver()

    def process_events(
        self,
        events: list[dict],
        job_run_id,
        job_id,
        pipeline_id=None,
        pipeline_run_id=None,
    ) -> dict:
        spark_application = self.resolver.events_resolver(events)
        spark_application.set_totals()

        peak_memory_usage = 0
        if spark_application.memory_per_executor:
            peak_memory_usage = (
                spark_application.executor_total.task_total.total_memory
                / spark_application.memory_per_executor
            ) * 100

        cpu_utilization = 0
        if spark_application.executor_total.cpu_uptime:
            cpu_utilization = (
                spark_application.executor_total.task_total.cpu_time
                / spark_application.executor_total.cpu_uptime
            ) * 100

        spark_job_run = {
            "id": job_run_id,
            "job_id": job_id,
            "pipeline_id": pipeline_id,
            "pipeline_run_id": pipeline_run_id,
            "cpu_utilization": cpu_utilization,
            "total_cpu_time_used": spark_application.executor_total.task_total.cpu_time,
            "num_of_executors": len(spark_application.executors),
            "total_memory_per_executor": spark_application.memory_per_executor,
            "total_bytes_read": spark_application.executor_total.task_total.bytes_read,
            "total_shuffle_bytes_read": spark_application.executor_total.task_total.shuffle_bytes_read,
            "total_bytes_written": spark_application.executor_total.task_total.bytes_written,
            "total_shuffle_bytes_written": spark_application.executor_total.task_total.shuffle_bytes_written,
            "total_cpu_uptime": spark_application.executor_total.cpu_uptime,
            "peak_memory_usage": peak_memory_usage,
            "total_cores_num": spark_application.executor_total.num_cores,
            "start_time": spark_application.start_time,
            "end_time": spark_application.end_time,
        }
        return spark_job_run
