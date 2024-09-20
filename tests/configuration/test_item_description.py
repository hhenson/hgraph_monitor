from hgraph import graph, TS, TSD, register_service, default_path, debug_print, set_record_replay_model, IN_MEMORY, \
    register_adaptor
from hgraph.adaptors.tornado.http_server_adaptor import http_server_adaptor_helper
from hgraph.test import eval_node

from hgraph_monitor.configuration.item_description import ItemDescription, item_description_impl, item_create_update, \
    item_descriptions, item_delete

from frozendict import frozendict as fd

ITEM_PATH = "ItemPath"

@graph
def crud_tester(create_update: TS[ItemDescription], remove_id: TS[str]) -> TSD[str, TS[ItemDescription]]:
    set_record_replay_model(IN_MEMORY)
    register_service(ITEM_PATH, item_description_impl)
    register_adaptor(default_path, http_server_adaptor_helper, port=8080)
    debug_print("create_update", item_create_update(ITEM_PATH, create_update))
    debug_print("remove", item_delete(ITEM_PATH, remove_id))
    return item_descriptions(ITEM_PATH)


def test_item_description():
    item_1 = ItemDescription(name="a", description="a desc")
    assert eval_node(crud_tester, [item_1], []) == [None, fd(a=item_1)]