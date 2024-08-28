import json
from dataclasses import dataclass
from enum import Enum

from hgraph import TS, combine, TSD, TSB, TimeSeriesSchema, format_, CompoundScalar, compute_node, to_json, from_json, \
    convert
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


@compute_node(overloads=to_json)
def to_json_task_request(ts: TS[TaskRequest]) -> TS[str]:
    ts = ts.value
    return f'{{ "task_name": "{ts.task_name}", "context": "{ts.context}" }}'


@compute_node(overloads=from_json)
def from_json_task_request(ts: TS[str]) -> TS[TaskRequest]:
    ts = ts.value
    d = json.loads(ts)
    return TaskRequest(**d)


@http_server_handler(url="/task/start")
def task_starting(request: TS[HttpRequest], states: TSD[str, TSB[TaskStatus]]) -> TS[HttpResponse]:
    request = convert[TS[HttpPostRequest]](request)
    request = from_json[TS[TaskRequest]](request.body)
    return combine[TS[HttpResponse]](
        status_code=200,
        body=format_('{{ "task_name": "{}",  "started": True }}', request.task_name)
    )
