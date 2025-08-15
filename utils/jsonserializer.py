import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import TypeVar, cast

from cattrs import Converter, unstructure

converter = Converter()


def datetime_structure_hook(data, T):
    return datetime.fromisoformat(data)


def timedelta_structure_hook(delta, T):
    return timedelta(seconds=int(float(delta)))


def datetime_unstructure_hook(val: datetime) -> str:
    """This hook will be registered for `datetime`s."""
    return val.isoformat()


def timedelta_unstructure_hook(val: timedelta) -> str:
    return str(val.total_seconds())


converter.register_structure_hook(cl=datetime, func=datetime_structure_hook)
converter.register_structure_hook(cl=timedelta, func=timedelta_structure_hook)
converter.register_unstructure_hook(cls=datetime, func=datetime_unstructure_hook)
converter.register_unstructure_hook(cls=timedelta, func=timedelta_unstructure_hook)


def dumpstr(obj):
    obj = unstructure(obj)
    return json.dumps(obj, indent=4, ensure_ascii=False)


def dump(obj, path: Path | str, objtype=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(file=path, mode="w", encoding="utf-8") as f:
        objtype = type(obj) if objtype is None else objtype
        obj = converter.unstructure(obj=obj, unstructure_as=objtype)
        json.dump(obj=obj, fp=f, indent=4, ensure_ascii=False)


T = TypeVar("T")


def load(path: Path | str, obj_or_cls=dict):
    with open(file=path, mode="r", encoding="utf-8") as f:
        ret_obj = json.load(fp=f)
        cl = obj_or_cls if isinstance(obj_or_cls, type) else type(obj_or_cls)
        return converter.structure(ret_obj, cl)


def load_type(path: Path | str, t: T):
    loaded = load(path, t)
    return cast(T, loaded)
