from datetime import timedelta
from typing import Callable

from hgraph import register_adaptor, default_path, TS, format_, combine, lag, MIN_TD, log_, sample, graph, \
    GraphConfiguration, EvaluationMode, evaluate_graph
from hgraph.adaptors.tornado.http_client_adaptor import http_client_adaptor_impl, http_client_adaptor
from hgraph.adaptors.tornado.http_server_adaptor import HttpResponse, HttpPostRequest
from hgraph.nodes import stop_engine


def monitor_client_registration():
    register_adaptor(default_path, http_client_adaptor_impl)


def send_task_start(base_url: TS[str], name: TS[str]) -> TS[HttpResponse]:
    url = format_("{}/task/start",base_url)
    body = format_('{{ "task_name": "{}" }}', name)
    request = combine[TS[HttpPostRequest]](
        url=sample(body, url),
        body=body
    )
    return http_client_adaptor(request)


def post_request_handler(response: TS[HttpResponse]):
    log_("Response status code: {} response body: {}", response.status_code, response.body)
    stop_engine(lag(response, MIN_TD*10))


def run_command(cmd: Callable, base_url: str, **kwargs):
    @graph
    def main_graph():
        monitor_client_registration()
        out = cmd(base_url=base_url, **kwargs)
        post_request_handler(out)

    config = GraphConfiguration(
        run_mode=EvaluationMode.REAL_TIME,
        end_time = timedelta(seconds=5),
    )
    evaluate_graph(main_graph, config)
