import fire

from hgraph_monitor.client.task_client import run_command, send_task_start


class MonitorClient:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"


class Task(MonitorClient):

    def __init__(self, host: str, port: int):
        super().__init__(host, port)

    def start(self, name: str):
        run_command(send_task_start, self.url, name=name)

    def stop(self, name: str):
        ...


class MonitorClientCLI:

    def __init__(self, host: str = "localhost", port: int = 8080):
        self.task = Task(host, port)


if __name__ == "__main__":
    fire.Fire(MonitorClientCLI)
