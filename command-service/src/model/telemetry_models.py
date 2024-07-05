import time
from dataclasses import dataclass
from uuid import uuid4

from dataclasses_json import dataclass_json

from .data_models import DatasetStatusType


@dataclass_json
@dataclass
class Actor:
    id: str = "SYSTEM"
    type: str = "User"


@dataclass_json
@dataclass
class Pdata:
    id: str
    ver: str | None = "1.0.0"


@dataclass_json
@dataclass
class Context:
    pdata: Pdata | None
    env: str
    sid: str = uuid4()


@dataclass_json
@dataclass
class Object:
    id: str
    type: str
    ver: str | None


@dataclass_json
@dataclass
class Property:
    property: str
    ov: str
    nv: str


@dataclass_json
@dataclass
class Transition:
    duration: str
    fromState: str | None = DatasetStatusType.ReadyToPublish.name
    toState: str | None = DatasetStatusType.Live.name
    timeunit: str = "milliseconds"


@dataclass_json
@dataclass
class Audit:
    props: list[Property]
    transition: Transition
    action: str = "dataset:publish"


@dataclass_json
@dataclass
class Telemetry:
    actor: Actor
    context: Context
    object: Object | None
    edata: Audit | None
    eid: str = "AUDIT"
    ets: int = time.time() * 1000
    ver: str = "1.0.0"
    mid: str = uuid4()
