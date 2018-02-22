# -*- caoding: utf-8 -*-
from functools import wraps

def singleton(cls):
    cache = {}
    @wraps(cls)
    def _wrap(*args, **kargs):
        if cls not in cache:
            return cache.setdefault(cls, cls(*args, **kargs))
        else:
            return cache[cls]
    return _wrap