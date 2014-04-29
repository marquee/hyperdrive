
from content        import ContentObjects, Container
from content.models import instanceFromRaw
from .main          import redisdb

from datetime       import datetime, timedelta

import json
import logging
import redis
import requests
import zlib



def load(s):
    return json.loads(s)


def dump(s):
    return json.dumps(s)

    

class ContentSet(object):
    """
    Public: Interface for accessing stories within redis

    """

    def __init__(self, *args, **kwargs):
        # # TODO: figure out what to do with redis connection object

        self.setkey = self.supersetkey
        self._redis = redisdb
        self._results = None

        if len(args) > 1 or len(kwargs) > 1:
            raise Exception("Only one set key allowed")

        if len(kwargs) == 0:
            if len(args) == 1 and len(kwargs) == 0:
                self.setkey = args[0]


    def __repr__(self):
        self.fetch()
        return repr(self.set_keys)

    def fetch(self, start=None, stop=None,**kwargs):

        # TODO: think this through more
        if self._results:
            if start != None and stop != None:
                return self._results[start:stop+1]
            else:
                return self._results

        if not start and not stop:
            start = 0
            stop  = -1

        self.set_keys = self._redis.zrevrange(self.setkey, start, stop)
        pipe = self._redis.pipeline()
        [pipe.hgetall(key) for key in self.set_keys]
        self._results = pipe.execute()
        return self._results

    def __or__(self, other):
        union_key = self.setkey + " | " + other.setkey
        self._redis.zunionstore(union_key, [self.setkey, other.setkey], aggregate="max")
        return self.__class__(union_key)

    def __sub__(self, other):
        newkey = self.setkey + " - " + other.setkey
        self._redis.zunionstore(
            newkey,
            {self.setkey:1, other.setkey: -1},
            aggregate="sum"
        )

        self._redis.zremrangebyscore(newkey, "-inf", 0)
        return self.__class__(newkey)

    def __and__(self, other):
        newkey = self.setkey + " & " + other.setkey
        self._redis.zinterstore(newkey, [self.setkey, other.setkey], aggregate='MAX')
        return self.__class__(newkey)

    def __getitem__(self, k):

        if isinstance(k, slice):
            start = k.start
            stop  = k.stop
            if k.start is None:
                start = 0

            if k.stop is None:
                stop = 0

            self.fetch(start=start, stop=stop-1)
            return self
        elif isinstance(k, int):
            stories = self.fetch(start=k, stop=k)
            if stories:
                s = self.klass(self._load(stories[0]['object']))
                return s
            else:
                raise IndexError
        else:
            raise TypeError

    def __len__(self):
        self.fetch()
        return len(self._results)
        # return self._redis.zcard(self.setkey)

    def __iter__(self):
        self.fetch()

        for s in self._results:
            yield self.klass(self._load(s['object']))


    def date_slice(self, start, end, offset=None, limit=None):
        self.set_keys = self._redis.zrangebyscore(
            self.setkey,
            start.strftime("%s"),
            end.strftime("%s"),
            start=offset,
            num=limit
        )

        pipe = self._redis.pipeline()
        [pipe.hgetall(key) for key in self.set_keys]
        self._results = pipe.execute()

        return self

    def _load(self, s):
        # what.the.fuck
        return instanceFromRaw(load(s))

    @classmethod
    def select(cls, **kwargs):
        if len(kwargs) == 0:
            return cls.all()

        OR  = lambda x,y : x | y
        AND = lambda x,y : x & y

        selected_sets = []
        excluded_sets = []

        for field, value in kwargs.items():
            queryitems = field.split("__")
            param      = queryitems[0]

            make_set = lambda p, v : cls(**{param: v})

            if len(queryitems) == 2:
                operator   = queryitems[1]
                if operator == "in": 
                    keys = map(lambda v: make_set(param,v), value)
                    selected_sets.append(reduce(OR, keys))

                elif operator == "nin":
                    keys = map(lambda v: make_set(param,v), value)
                    excluded_sets.append(reduce(OR, keys))

                elif operator == "ne":
                    excluded_sets.append(make_set(param, value))

            else:
                selected_sets.append(make_set(param, value))
        # TODO: 
        # Needs to handle exclusion only
        computation = None
        if selected_sets:
            computation = reduce(AND, selected_sets)

        if excluded_sets:
            subtractand = reduce(OR, excluded_sets)
            if computation:
                computation = computation - subtractand
            else:
                computation = cls("stories") - subtractand
        return computation

    def count(self):
        return self._redis.zcard(self.setkey)

    @classmethod
    def get(cls, slug):
        key = "{}:{}".format(cls.role, slug)
        container_hash = redisdb.hgetall(key)

        if not container_hash:
            return None

        obj = container_hash['object']
            
        
        return cls.klass(Container(load(obj)))

    # TODO: IMPLEMENT
    @classmethod
    def empty(cls):
        self.cls()
        s.setkey = None
        return s

    @classmethod
    def all(cls):
        return cls(cls.supersetkey)
    