from dataclasses import dataclass
from enum import IntEnum

from hgraph import CompoundScalar, TS, service_impl, TSD, request_reply_service, \
    get_service_inputs, replay_const, TimeSeriesSchema, TSB, compute_node, \
    reference_service, set_service_output, REMOVE, record, default_path, debug_print
from hgraph.adaptors.tornado._rest_handler import RestRequest, RestResponse, rest_handler, RestCreateRequest, \
    RestCreateResponse, RestResultEnum, RestDeleteRequest, RestUpdateRequest, RestUpdateResponse, RestDeleteResponse, \
    RestReadRequest, RestReadResponse, RestListRequest, RestListResponse


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


@reference_service
def item_descriptions(path: str = default_path) -> TSD[str, TS[ItemDescription]]:
    """
    Subscribe to the item description
    """


@request_reply_service
def item_create_update(path: str, item: TS[ItemDescription]) -> TS[str]:
    ...


@request_reply_service
def item_delete(path: str, item_id: TS[str]) -> TS[str]:
    ...


@service_impl(interfaces=(item_descriptions, item_create_update, item_delete))
def item_description_impl(path: str):
    """Implements the item description service"""
    initial_state = replay_const("items", TSD[str, TS[ItemDescription]], recordable_id="MonitorAPI")
    out = _apply_crud_op(
        create_update_item=get_service_inputs(path, item_create_update).item,
        remove_item_id=get_service_inputs(path, item_delete).item_id,
        initial_state=initial_state
    )
    debug_print("ItemDescription", out)
    set_service_output(path, item_descriptions, out.items)
    set_service_output(path, item_create_update, out.create_results)
    set_service_output(path, item_delete, out.remove_results)
    record(out.items, "items", recordable_id="MonitorAPI")


class _ItemDescriptionCrudResult(TimeSeriesSchema):
    response: TSD[int, TS[RestResponse[ItemDescription]]]
    create_results: TSD[int, TS[str]]
    remove_results: TSD[int, TS[str]]
    items: TSD[str, TS[ItemDescription]]


@rest_handler(url="/config/item_description", data_type=ItemDescription)
@compute_node(valid=tuple())
def _apply_crud_op(
        request: TSD[int, TS[RestRequest[ItemDescription]]],
        create_update_item: TSD[int, TS[ItemDescription]],
        remove_item_id: TSD[int, TS[str]],
        initial_state: TSD[str, TS[ItemDescription]],
        _output: TSB[_ItemDescriptionCrudResult] = None
) -> TSB[_ItemDescriptionCrudResult]:
    responses = {}
    create_update_results = {}
    remove_item_results = {}
    items_updates = {}
    if initial_state.modified:
        # This will potentially tick once when the graph starts with the last recorded state.
        # So we initialise our output with this state.
        items_updates = dict(initial_state.value)
    items: TSD[str, TS[ItemDescription]] = _output.value.items_ if _output.valid else {}
    for i, item_ts in create_update_item.modified_items():
        item: ItemDescription = item_ts.value
        if item.name in items:
            kwargs = items[item.name].value.to_dict() | item.to_dict()
            items_updates[item.name] = ItemDescription(**kwargs)
            create_update_results[i] = "Updated"
        else:
            items_updates[item.name] = item
            create_update_results[i] = "Created"

    for i, item_id_ts in remove_item_id.modified_items():
        item_id: str = item_id_ts.value
        if item_id in items:
            if item_id in items:
                items_updates[item_id] = REMOVE
                remove_item_results[i] = "Removed"
            else:
                create_update_results[i] = "Not found"

    for i, request_ts in request.modified_items():
        request: RestRequest[ItemDescription] = request_ts.value
        if isinstance(request, RestCreateRequest):
            id, item = _validate_id(request)
            if id not in items:
                items_updates[id] = item
                responses[i] = RestCreateResponse[RestResponse[ItemDescription]](status=RestResultEnum.CREATED,
                                                                                 value=item)
            else:
                responses[i] = RestCreateResponse[RestResponse[ItemDescription]](status=RestResultEnum.CONFLICT,
                                                                                 value=item)
        elif isinstance(request, RestUpdateRequest):
            id, item = _validate_id(request)
            if id not in items:
                responses[i] = RestUpdateResponse[RestResponse[ItemDescription]](status=RestResultEnum.NOT_FOUND,
                                                                                 value=item)
            else:
                kwargs = items[id].value.to_dict() | item.to_dict()
                items_updates[id] = ItemDescription(**kwargs)
                responses[i] = RestUpdateResponse[RestResponse[ItemDescription]](status=RestResultEnum.OK, value=item)
        elif isinstance(request, RestDeleteRequest):
            id = request.id
            if id in items:
                items_updates[id] = REMOVE
                responses[i] = RestDeleteResponse[RestResponse[ItemDescription]](status=RestResultEnum.OK)
            else:
                responses[i] = RestDeleteResponse[RestResponse[ItemDescription]](status=RestResultEnum.NOT_FOUND)
        elif isinstance(request, RestReadRequest):
            id = request.id
            if id in items:
                responses[i] = RestReadResponse[RestResponse[ItemDescription]](status=RestResultEnum.OK,
                                                                               value=items[id])
            else:
                responses[i] = RestReadResponse[RestResponse[ItemDescription]](status=RestResultEnum.NOT_FOUND)
        elif isinstance(request, RestListRequest):
            responses[i] = RestListResponse[RestResponse[ItemDescription]](status=RestResultEnum.OK,
                                                                           ids=tuple(items.keys()))

    for i in create_update_item.removed_keys():
        create_update_results[i] = REMOVE
    for i in remove_item_id.removed_keys():
        remove_item_results[i] = REMOVE

    return {'response': responses, 'create_results': create_update_results, 'remove_results': remove_item_results,
            'items': items_updates}


def _validate_id(request):
    id = request.id
    item = request.value
    assert id == item.name, f"Item id '{id}' does not match item name '{item.name}'"
    return id, item
