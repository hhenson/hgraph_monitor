from dataclasses import dataclass
from enum import IntEnum

from hgraph import CompoundScalar, subscription_service, TS, service_impl, TSD, request_reply_service, \
    get_service_inputs, replay_const, map_, feedback, merge, pass_through, graph, TimeSeriesSchema, TSB, compute_node, \
    rekey, reference_service, set_service_output, REMOVE, record

from hgraph_monitor.configuration.crud import CrudOperation, CrudEvent


class Level(IntEnum):
    UNKNOWN = 0
    OK = 1
    WARNING = 2
    ERROR = 3


@dataclass
class ItemDescription(CompoundScalar):
    name: str
    description: str = None
    owner: str = None
    email_notification_list: tuple[str] = None
    notification_trigger_level: Level = None


@dataclass
class ItemDescriptionCrud(CrudEvent):
    item_description: ItemDescription = None


@reference_service
def item_description(path: str) -> TSD[str, TS[ItemDescription]]:
    """
    Subscribe to the item description
    """


@request_reply_service
def crud_item_description(path: str, event: TS[ItemDescriptionCrud]) -> TS[bool]:
    """Create or update the item descriptions"""


@service_impl(interfaces=(item_description, crud_item_description))
def item_description_impl(path: str):
    """Implements the item description service"""
    initial_state = replay_const[TSD[str, TS[ItemDescription]]]("items", "MonitorAPI")
    out = _apply_crud_op(get_service_inputs(path, crud_item_description).event, initial_state)
    set_service_output(path, item_description, out.items)
    set_service_output(path, crud_item_description, out.results)
    record(out.items, "items", "MonitorAPI")


class ItemDescriptionCrudResult(TimeSeriesSchema):
    results: TSD[int, TS[bool]]
    items: TSD[str, TS[ItemDescription]]


@compute_node(active=("event",))
def _apply_crud_op(events: TSD[int, TS[ItemDescriptionCrud]],
                   _out: TSB[ItemDescriptionCrudResult] = None) -> TSB[ItemDescriptionCrudResult]:
    results = {}
    items_updates= {}
    items = _out.items
    for i, event in events.modified_items():
        event: ItemDescriptionCrud = event.value
        match event.operation:
            case CrudOperation.CREATE:
                if event.id in items:
                    results[i] = False
                else:
                    results[i] = True
                    items_updates[event.id] = event.item_description
            case CrudOperation.UPDATE:
                if event.id in items:
                    results[i] = True
                    kwargs = items[event.id].value.to_dict() | event.item_description.to_dict()
                    items_updates[event.id] = ItemDescription(**kwargs)
                else:
                    results[i] = False
            case CrudOperation.DELETE:
                if event.id in items:
                    results[i] = True
                    items_updates[event.id] = REMOVE
                else:
                    results[i] = False

    for i in events.removed_keys():
        results[i] = REMOVE

    return {'results': results, 'items': items_updates}


