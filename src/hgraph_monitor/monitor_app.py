import argparse
from datetime import datetime

from hgraph import graph, register_adaptor, GraphConfiguration, EvaluationMode, evaluate_graph, default_path
from hgraph.adaptors.tornado.http_server_adaptor import http_server_adaptor_helper

from hgraph_monitor.tasks.task_plugin import register_task_plugin


@graph
def monitor_app(port: int = 8080):
    register_adaptor(default_path, http_server_adaptor_helper, port=port)
    register_task_plugin()


def main(port: int = 8080):
    config = GraphConfiguration(
        run_mode=EvaluationMode.REAL_TIME,
        start_time=datetime.utcnow(),
    )
    evaluate_graph(monitor_app, config, port=port)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run monitor app")
    parser.add_argument("-p", "--port", type=int, default=8080)

    args = parser.parse_args()

    main(args.port)
