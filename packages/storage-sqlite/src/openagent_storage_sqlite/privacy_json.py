"""Canonical JSON subtree selectors, including serialized engine JSON copies."""
import json


def get(root,path):
    value=root
    for component in path:
        if isinstance(value,str):
            value=json.loads(value)
        value=value[component]
    return value


def replace(root,path,value):
    if not path:
        return value
    if isinstance(root,str):
        return json.dumps(replace(json.loads(root),path,value),sort_keys=True,separators=(',',':'),ensure_ascii=False)
    component=path[0]
    root[component]=replace(root[component],path[1:],value)
    return root
