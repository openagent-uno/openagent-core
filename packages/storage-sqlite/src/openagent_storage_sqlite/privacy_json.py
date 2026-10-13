"""Canonical JSON subtree selectors, including serialized engine JSON copies."""
import json
from openagent_core.content_privacy import JsonStringBoundary,decode_fragment_json


def _boundary(component):
    return isinstance(component,JsonStringBoundary) or component=={'decode_json':True}


def encode_path(path):
    return tuple({'decode_json':True} if isinstance(component,JsonStringBoundary) else component for component in path)


def get(root,path):
    value=root
    for component in path:
        if _boundary(component):
            value=decode_fragment_json(value)
            continue
        if isinstance(value,str):
            value=decode_fragment_json(value)
        value=value[component]
    return value


def replace(root,path,value):
    if not path:
        return value
    if _boundary(path[0]):
        return json.dumps(replace(decode_fragment_json(root),path[1:],value),sort_keys=True,separators=(',',':'),ensure_ascii=False)
    if isinstance(root,str):
        return json.dumps(replace(decode_fragment_json(root),path,value),sort_keys=True,separators=(',',':'),ensure_ascii=False)
    component=path[0]
    root[component]=replace(root[component],path[1:],value)
    return root
