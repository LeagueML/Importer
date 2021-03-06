from typing import ByteString, Optional, Type, TypeVar
import pickle
import json


T = TypeVar("T")


def to_bytes(obj) -> ByteString:
    '''Convert a python object to byte string.'''
    return pickle.dumps(obj)


def from_bytes(obj, class_of_t: Optional[Type[T]] = None) -> T:
    '''Convert a byte string to python object.'''
    return pickle.loads(obj)


def safejson(content):
    try:
        return json.loads(content)
    except json.decoder.JSONDecodeError:
        return content
