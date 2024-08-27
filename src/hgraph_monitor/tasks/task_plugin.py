from hgraph import graph, const, TSB, TSD
from hgraph_monitor.tasks.task_api import task_starting, TaskStatus, TaskStatusEnum

from frozendict import frozendict as fd


@graph
def register_task_plugin():
    states = const(fd({'a': fd(status=TaskStatusEnum.STARTED, reason=''),}), TSD[str, TSB[TaskStatus]])
    task_starting(states=states)
