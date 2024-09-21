import json
from dataclasses import dataclass
from enum import Enum

from hgraph import TS, combine, TSD, TSB, TimeSeriesSchema, format_, CompoundScalar, compute_node, to_json, from_json, \
    convert, if_, if_true, sample, debug_print, merge
from hgraph.adaptors.tornado.http_server_adaptor import HttpRequest, HttpResponse, http_server_handler, HttpPostRequest


class TaskStatusEnum(Enum):
    UNKNOWN = "Unknown"
    PENDING = "Pending"
    STARTED = "Started"
    SUCCESS = "Success"
    FAILED = "Failed"


@dataclass(frozen=True)
class TaskStatus(TimeSeriesSchema):
    status: TS[TaskStatusEnum]
    reason: TS[str]


@dataclass(frozen=True)
class TaskRequest(CompoundScalar):
    task_name: str
    context: str = ""


@dataclass(frozen=True)
class TaskStatusEvent(CompoundScalar):
    task_name: str
    status: TaskStatusEnum
    reason: str


@dataclass(frozen=True)
class TaskStartingResult(TimeSeriesSchema):
    response: TS[HttpResponse]
    task_event: TS[TaskStatusEvent]


@http_server_handler(url="/task/start")
def task_starting(request: TS[HttpRequest], states: TSD[str, TSB[TaskStatus]]) -> TSB[TaskStartingResult]:
    request = convert[TS[HttpPostRequest]](request)
    request = from_json[TS[TaskRequest]](request.body)

    # if states already include the task name
    task_status = states[request.task_name]
    not_started = merge(task_status.status, TaskStatusEnum.UNKNOWN) != TaskStatusEnum.STARTED

    task_event = combine[TS[TaskStatusEvent]](
        task_name=sample(if_true(not_started), request.task_name),
        status=TaskStatusEnum.STARTED,
        reason="",
    )
    debug_print("TE", task_event)

    ok = combine[TS[HttpResponse]](
        status_code=200,
        body=format_('{{ "task_name": "{}",  "started": True }}', request.task_name)
    )

    return combine[TSB[TaskStartingResult]](response=ok, task_event=task_event)
