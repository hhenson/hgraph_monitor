from dataclasses import dataclass
from datetime import timedelta, time

from frozendict import frozendict as fd
from hgraph import graph, const, TSB, TSD, emit, debug_print, CompoundScalar

from hgraph_monitor.tasks.task_api import task_starting, TaskStatus, TaskStatusEnum


@dataclass
class TaskConfiguration(CompoundScalar):
    name: str
    expected_start: time
    expected_duration: timedelta
    failed_delay: timedelta
    failed_time: time
    predecessors: tuple[str, ...]




@graph
def register_task_plugin():
    states = const(fd({'a': fd(status=TaskStatusEnum.STARTED, reason=''), }), TSD[str, TSB[TaskStatus]])
    task_event = emit(task_starting(states=states).task_event).value
    debug_print("task events", task_event.task_name)
