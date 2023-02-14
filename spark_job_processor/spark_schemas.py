from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Type

from pydantic import BaseModel, Field


class BaseSpark(BaseModel):
    def __getitem__(self, name):
        return self.__getattribute__(name)

    def __setitem__(self, name, value):
        self.__setattr__(name, value)

    @staticmethod
    def calc_total(items: list[BaseSpark], return_type: Type[BaseSpark]) -> BaseSpark:
        """
        Calculates the total of specified keys for a list of `items`.
        The result is stored in a `return_type` object.

        Parameters
        ----------
        items : list of BaseSpark objects
            A list of `BaseSpark` objects to be processed
        return_type : Type[BaseSpark]
            The type of object to store the result. It must be a subclass of `BaseSpark`

        Returns
        -------
        total_item : BaseSpark
            An object of type `return_type` that contains the calculated total of specified keys

        """

        max_keys = [
            "jvm_memory",
            "python_memory",
            "other_memory",
            "total_memory",
        ]
        total_item = return_type()
        for item in items:
            if not isinstance(item, return_type):
                raise ValueError(
                    f"Each item in the 'items' list must be an instance of 'return_type'=`{return_type.__name__}` "
                    f"and not `{type(item).__name__}`."
                )
            for key, value in item:
                if key in max_keys:
                    total_item[key] = max(total_item[key], value)
                elif isinstance(value, (int, float)):
                    total_item[key] += value

        return total_item


class SparkTask(BaseSpark):
    cpu_time: int = 0
    bytes_read: int = 0
    records_read: int = 0
    bytes_written: int = 0
    records_written: int = 0
    shuffle_remote_bytes_read: int = 0
    shuffle_local_bytes_read: int = 0
    shuffle_bytes_read: int = 0
    shuffle_bytes_written: int = 0
    jvm_memory: int = 0
    python_memory: int = 0
    other_memory: int = 0
    total_memory: int = 0
    total_shuffle_bytes_read: int = 0

    def set_totals(self):
        self.total_memory = sum(
            [self.jvm_memory, self.python_memory, self.other_memory]
        )
        self.total_shuffle_bytes_read = sum(
            [self.shuffle_remote_bytes_read, self.shuffle_local_bytes_read]
        )


class SparkExecutor(BaseSpark):
    start_time: datetime | None = None
    end_time: datetime | None = None
    num_cores: int = 0
    cpu_uptime: int = 0
    tasks: list[SparkTask] = Field(default_factory=list)
    task_total: SparkTask | None = None

    def set_totals(self):
        if self.start_time and self.end_time:
            self.cpu_uptime = (
                self.end_time - self.start_time
            ).total_seconds() * self.num_cores
        list(map(lambda task: task.set_totals(), self.tasks))
        self.task_total = self.calc_total(self.tasks, SparkTask)


class SparkApplication(BaseSpark):
    start_time: datetime | None = None
    end_time: datetime | None = None
    memory_per_executor: float = 0.0
    executors: dict[str, SparkExecutor] = defaultdict(SparkExecutor)
    executor_total: SparkExecutor | None = None

    def set_totals(self):
        executors = self.executors.values()
        list(map(lambda executor: executor.set_totals(), executors))
        self.executor_total = self.calc_total(executors, SparkExecutor)
        self.executor_total.task_total = self.calc_total(
            (executor.task_total for executor in executors), SparkTask
        )
