from dataclasses import dataclass
from enum import IntEnum

from hgraph import CompoundScalar


class CrudOperation(IntEnum):
    CREATE = 1
    UPDATE = 2
    DELETE = 3


@dataclass
class CrudEvent(CompoundScalar):
    operation: CrudOperation
    id: str
