from app            import settings
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
    COMPRESS = getattr(settings, "HYPERDRIVE_COMPRESS", False)
    if COMPRESS:
        return json.loads(zlib.decompress(s))
    else:
        return json.loads(s)


def dump(s):
    COMPRESS = getattr(settings, "HYPERDRIVE_COMPRESS", False)
    if COMPRESS:
        return zlib.compress(json.dumps(s), 9)
    else:
        return json.dumps(s)

    

class StorySet(object):
    """
    Public: Interface for accessing stories within redis

    """

    def __init__(self, *args, **kwargs):
        # # TODO: figure out what to do with redis connection object

        self._redis = redisdb
        self._results = None

        if len(args) > 1 or len(kwargs) > 1:
            raise Exception("Only one set key allowed")

        if len(kwargs) == 0:
            if len(args) == 0:
                self.setkey = "stories" # all stories set. you generally don't want this entire thing
            elif len(args) == 1 and len(kwargs) == 0:
                self.setkey = args[0]

        # this should be based on the mapping
        tag           = kwargs.pop("tags", None)
        category      = kwargs.pop("category", None)
        issue         = kwargs.pop("issue", None)

        if tag:
            self.setkey = "tags:{}:stories".format(tag)
        elif category:
            self.setkey = "category:{}:stories".format(category)
        elif issue:
            self.setkey = "issue:{}:stories".format(issue)

        from .models import Story
        self.klass = Story

        if not self.setkey:
            raise Exception("Set does not exist.")

    def __repr__(self):
        self.fetch()
        return repr(self.set_story_keys)

    def fetch(self, start=None, stop=None,**kwargs):
        # TODO: think this through more
        if self._results:
            if start != None and stop != None:
                print "STARTSTOP", start, stop, len(self._results[start:stop+1])
                return self._results[start:stop+1]
            else:
                return self._results

        if not start and not stop:
            start = 0
            stop  = -1

        print start, stop
        self.set_story_keys = self._redis.zrevrange(self.setkey, start, stop)
        pipe = self._redis.pipeline()
        [pipe.hgetall(key) for key in self.set_story_keys]
        self._results = pipe.execute()
        return self._results

    def __or__(self, other):
        union_key = self.setkey + " | " + other.setkey
        self._redis.zunionstore(union_key, [self.setkey, other.setkey], aggregate="max")
        return StorySet(union_key)

    def __sub__(self, other):
        newkey = self.setkey + " - " + other.setkey
        self._redis.zunionstore(
            newkey,
            {self.setkey:1, other.setkey: -1},
            aggregate="sum"
        )

        self._redis.zremrangebyscore(newkey, "-inf", 0)
        return StorySet(newkey)

    def __and__(self, other):
        newkey = self.setkey + " & " + other.setkey
        self._redis.zinterstore(newkey, [self.setkey, other.setkey], aggregate='MAX')
        return StorySet(newkey)

    def __getitem__(self, k):
        start = k.start
        stop  = k.stop
        if isinstance(k, slice):

            if k.start is None:
                start = 0

            if k.stop is None:
                stop = 0

            self.fetch(start=start, stop=stop-1)
            return self
        elif isinstance(k, int):
            stories = selyf.fetch(start=k, stop=k)
            if stories:
                return self._load(stories[0]['object'])
            else:
                return []
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
                # print cls("stories")
                computation = cls("stories") - subtractand
        return computation 

    def count(self):
        return self._redis.zcard(self.setkey)

    @classmethod
    def get(cls, slug):
        story_key = "story:{}".format(slug)
        story_obj = redisdb.hgetall(story_key)['object']
        return load(story_obj)

    # TODO: IMPLEMENT
    @classmethod
    def empty(cls):
        s = StorySet()
        s.setkey = None
        return s

    @classmethod
    def all(cls):
        return cls("stories")
    
    def histogram(self, field, n=10):
        """
        field:tags
        field:category
        """

        field_key = "field:{}".format(field)
        histogram_key = "histogram:{}:{}:count".format(self.setkey, field)

        if not self._redis.exists(histogram_key):
            self.fetch()
            story_tags_keys = map( lambda s: "{}:{}".format(s,field), self.set_story_keys )
        
            self._redis.zunionstore(histogram_key, story_tags_keys)

        field_counts = []
        for f in self._redis.zrevrange(histogram_key, 0, n, withscores=True):
            field_name = self._redis.hget(field_key, f[0])
            field_counts.append({
                "count" : f[1],
                "name"  : field_name,
                "slug"  : f[0]
            })        
        return field_counts