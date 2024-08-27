from hgraph import TS, combine
from hgraph.adaptors.tornado.http_server_adaptor import HttpRequest, HttpResponse, http_server_handler


@http_server_handler(url="/tasks/start")
def task_starting(request: TS[HttpRequest]) -> TS[HttpResponse]:
    return combine[TS[HttpResponse]](status_code=200, body="Started")

