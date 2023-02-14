from unittest import TestCase

from spark_job_processor.models import (
    BaseModel,
    Application,
    Executor,
    Task,
)


class TestSchemas(TestCase):
    def test_base_model_calc_total(self):
        base_model = BaseModel()
        base_model.calc_total([], BaseModel)

    def test_base_model_calc_total_not_the_same_types(self):
        base_model = BaseModel()
        with self.assertRaises(ValueError) as context:
            base_model.calc_total([{}], BaseModel)

        self.assertEqual(
            str(context.exception),
            "Each item in the 'items' list must be an instance of 'return_type'=`BaseModel` and not `dict`.",
        )

    def test_task_set_totals(self):
        task = Task(
            jvm_memory=1,
            python_memory=1,
            other_memory=1,
        )
        task.set_totals()
        assert task.total_memory == 3

    def test_executor_set_totals(self):
        executor = Executor(
            tasks=[
                Task(
                    cpu_time=1,
                    jvm_memory=1,
                    python_memory=1,
                    other_memory=1,
                ),
                Task(cpu_time=2, bytes_read=2, records_read=2, other_memory=2),
            ]
        )
        executor.set_totals()

        assert executor.task_total.cpu_time == 3
        assert executor.task_total.jvm_memory == 1
        assert executor.task_total.python_memory == 1
        assert executor.task_total.other_memory == 2
        assert executor.task_total.total_memory == 3
        assert executor.task_total.bytes_read == 2
        assert executor.task_total.records_read == 2

    def test_application_set_totals(self):
        application = Application()
        application.executors["1111"] = Executor(
            num_cores=10,
            tasks=[
                Task(
                    cpu_time=1,
                    jvm_memory=1,
                    python_memory=1,
                    other_memory=1,
                ),
                Task(
                    cpu_time=2,
                    shuffle_bytes_read=2,
                    bytes_read=2,
                    records_read=2,
                    other_memory=2,
                ),
            ],
        )
        application.executors["2222"] = Executor(
            num_cores=10,
            tasks=[
                Task(
                    cpu_time=3,
                    jvm_memory=3,
                    python_memory=3,
                    other_memory=3,
                ),
                Task(
                    cpu_time=4,
                    bytes_read=4,
                    records_read=4,
                    shuffle_bytes_written=4,
                    other_memory=4,
                ),
            ],
        )
        application.set_totals()
        assert application.executor_total.num_cores == 20
        assert application.executor_total.task_total.cpu_time == 10
        assert application.executor_total.task_total.bytes_read == 6
        assert application.executor_total.task_total.records_read == 6
        assert application.executor_total.task_total.shuffle_bytes_read == 2
        assert application.executor_total.task_total.shuffle_bytes_written == 4
        assert application.executor_total.task_total.jvm_memory == 3
        assert application.executor_total.task_total.python_memory == 3
        assert application.executor_total.task_total.other_memory == 4
        assert application.executor_total.task_total.total_memory == 9
