from collections import defaultdict
from datetime import datetime
from pydantic import BaseModel, Field




class Model(BaseModel):

    def __getitem__(self, item):
        return self.__dict__[item]

    def __setitem__(self, item, value):
        self.__dict__[item] = value


def get_total_model(models: list[Model]) -> Model:
    max_keys = ['jvm_peak_memory', 'python_peak_memory', 'other_peak_memory', 'total_peak_memory']
    total_model = models[0].copy()
    for model in models[1:]:
        for key, value in model:
            if key in max_keys:
                total_model[key] = max(total_model[key], value)
            elif isinstance(value, (int, float)):
                total_model[key] += value

    return total_model


class SparkTask(Model):
    cpu_time: int = 0
    bytes_read: int = 0
    records_read: int = 0
    bytes_written: int = 0
    records_written: int = 0
    remote_bytes_read: int = 0
    local_bytes_read: int = 0
    shuffle_bytes_read: int = 0
    shuffle_bytes_written: int = 0
    jvm_peak_memory: int = 0
    python_peak_memory: int = 0
    other_peak_memory: int = 0
    total_peak_memory: int = 0

    def set_totals(self):
        self.total_peak_memory = sum([
            self.jvm_peak_memory, self.python_peak_memory, self.other_peak_memory
        ])


class SparkExecutor(Model):
    start_time: datetime | None = None
    end_time: datetime | None = None
    num_cores: int = 0
    tasks: list[SparkTask] = Field(default_factory=list)
    task_total: SparkTask | None = None

    def set_totals(self):
        list(map(lambda task: task.set_totals(), self.tasks))
        self.task_total = get_total_model(self.tasks)


class SparkApplication(Model):
    start_time: datetime | None = None
    end_time: datetime | None = None
    memory_per_executor: float = 0.0
    executors: dict[str, SparkExecutor] = defaultdict(SparkExecutor)
    executor_total: SparkExecutor | None = None

    def set_totals(self):
        executors = list(self.executors.values())
        list(map(lambda executor: executor.set_totals(), executors))
        self.executor_total = get_total_model(executors)
        self.executor_total.task_total = get_total_model(list(executor.task_total for executor in executors))
