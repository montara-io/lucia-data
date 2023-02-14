from spark_job_processor.events_resolver import EventsResolver


class EventsProcessor:
    def __init__(self) -> None:
        self.resolver: EventsResolver = EventsResolver()

    def process_events(
        self,
        events: list[dict],
        job_run_id,
        job_id,
        pipeline_id=None,
        pipeline_run_id=None,
    ) -> dict:
        application = self.resolver.events_resolver(events)
        application.set_totals()
        total_cpu_time_used = round(
            application.executor_total.task_total.cpu_time / 1e9, 4
        )

        peak_memory_usage = 0
        if application.memory_per_executor:
            peak_memory_usage = round(
                (
                    application.executor_total.task_total.total_memory
                    / application.memory_per_executor
                )
                * 100,
                4,
            )

        cpu_utilization = 0
        if application.executor_total.cpu_uptime:
            cpu_utilization = round(
                (total_cpu_time_used / application.executor_total.cpu_uptime)
                * 100,
                4,
            )

        processed_data = {
            "id": job_run_id,
            "job_id": job_id,
            "pipeline_id": pipeline_id,
            "pipeline_run_id": pipeline_run_id,
            "cpu_utilization": cpu_utilization,
            "total_cpu_time_used": total_cpu_time_used,
            "num_of_executors": len(application.executors),
            "total_memory_per_executor": application.memory_per_executor,
            "total_bytes_read": application.executor_total.task_total.bytes_read,
            "total_shuffle_bytes_read": application.executor_total.task_total.total_shuffle_bytes_read,
            "total_bytes_written": application.executor_total.task_total.bytes_written,
            "total_shuffle_bytes_written": application.executor_total.task_total.shuffle_bytes_written,
            "total_cpu_uptime": application.executor_total.cpu_uptime,
            "peak_memory_usage": peak_memory_usage,
            "total_cores_num": application.executor_total.num_cores,
            "start_time": application.start_time,
            "end_time": application.end_time,
        }
        return processed_data
