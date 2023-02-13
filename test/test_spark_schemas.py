from unittest import TestCase

from spark_job_processor.spark_schemas import (
    BaseSpark,
    SparkApplication,
    SparkExecutor,
    SparkTask,
)


class TestSparkSchemas(TestCase):
    def test_base_spark_calc_total(self):
        base_spark = BaseSpark()
        base_spark.calc_total([], BaseSpark)

    def test_base_spark_calc_total_not_the_same_types(self):
        base_spark = BaseSpark()
        with self.assertRaises(ValueError) as context:
            base_spark.calc_total([{}], BaseSpark)

        self.assertEqual(
            str(context.exception),
            "Each item in the 'items' list must be an instance of 'return_type'=`BaseSpark` and not `dict`.",
        )

    def test_spark_task_set_totals(self):
        spark_task = SparkTask(
            jvm_memory=1,
            python_memory=1,
            other_memory=1,
        )
        spark_task.set_totals()
        assert spark_task.total_memory == 3

    def test_spark_executor_set_totals(self):
        spark_executor = SparkExecutor(
            tasks=[
                SparkTask(
                    cpu_time=1,
                    jvm_memory=1,
                    python_memory=1,
                    other_memory=1,
                ),
                SparkTask(cpu_time=2, bytes_read=2, records_read=2, other_memory=2),
            ]
        )
        spark_executor.set_totals()

        assert spark_executor.task_total.cpu_time == 3
        assert spark_executor.task_total.jvm_memory == 1
        assert spark_executor.task_total.python_memory == 1
        assert spark_executor.task_total.other_memory == 2
        assert spark_executor.task_total.total_memory == 3
        assert spark_executor.task_total.bytes_read == 2
        assert spark_executor.task_total.records_read == 2

    def test_spark_application_set_totals(self):
        spark_application = SparkApplication()
        spark_application.executors["1111"] = SparkExecutor(
            num_cores=10,
            tasks=[
                SparkTask(
                    cpu_time=1,
                    jvm_memory=1,
                    python_memory=1,
                    other_memory=1,
                ),
                SparkTask(
                    cpu_time=2,
                    shuffle_bytes_read=2,
                    bytes_read=2,
                    records_read=2,
                    other_memory=2,
                ),
            ],
        )
        spark_application.executors["2222"] = SparkExecutor(
            num_cores=10,
            tasks=[
                SparkTask(
                    cpu_time=3,
                    jvm_memory=3,
                    python_memory=3,
                    other_memory=3,
                ),
                SparkTask(
                    cpu_time=4,
                    bytes_read=4,
                    records_read=4,
                    shuffle_bytes_written=4,
                    other_memory=4,
                ),
            ],
        )
        spark_application.set_totals()
        assert spark_application.executor_total.num_cores == 20
        assert spark_application.executor_total.task_total.cpu_time == 10
        assert spark_application.executor_total.task_total.bytes_read == 6
        assert spark_application.executor_total.task_total.records_read == 6
        assert spark_application.executor_total.task_total.shuffle_bytes_read == 2
        assert spark_application.executor_total.task_total.shuffle_bytes_written == 4
        assert spark_application.executor_total.task_total.jvm_memory == 3
        assert spark_application.executor_total.task_total.python_memory == 3
        assert spark_application.executor_total.task_total.other_memory == 4
        assert spark_application.executor_total.task_total.total_memory == 9
