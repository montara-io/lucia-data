from abc import ABC, abstractmethod
from pathlib import Path

from common.utils import read_json


class EventsResolverBase(ABC):
    def __init__(self, path_events_config: str | Path, event_field_name: str) -> None:
        self.events_config = read_json(path_events_config)
        self.event_field_name = event_field_name

    @abstractmethod
    def events_resolver(self, events: list[dict]):
        """Resolving the events"""

    def find_value_in_event(self, event: str, field_name: str):
        for value in self.events_config[event[self.event_field_name]][field_name]:
            event = event[value]
        return event
