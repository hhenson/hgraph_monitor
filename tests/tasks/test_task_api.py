from hgraph import graph, TS, from_json, to_json
from hgraph.test import eval_node

from hgraph_monitor.tasks.task_api import TaskRequest


def test_json():

    @graph
    def g(ts: TS[TaskRequest]) -> TS[TaskRequest]:
        return from_json[TS[TaskRequest]](to_json(ts))

    assert eval_node(g, [TaskRequest(task_name="a")]) == [TaskRequest(task_name="a")]
