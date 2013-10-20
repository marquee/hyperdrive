from datetime    import datetime
from dateutil    import parser

import requests, json, zlib

# TODO THIS IS BAD
from app.data_loader  import content_objects

class Denormalizer(object):
    """
    Public: A class to help denormalize the data

    args:
    * `redis` - the redis object
    * `fields` - A list of strings representing fields for  querying
    * `prep_json_funcs` - (optional) A list of functions that will be called before the json is stored. Will be passed the json object.
    * `post_save_callbacks` - (optional) A list of functions to call after a story is processed. They will be passed the story key, story object and the redis object. 
    * `final_callbacks` - A list of functions to call during the finalization step. They will be passed the redis object.


    Explanation of Keys and the data they store:
    stories - sorted set of all story slugs scored/ordered
              by first_published_date in unix time

    story:<story_slug> - hash that has store_at date and json of the story object

    categories - a hash mapping category slug to json of
                category composition

    category:<category_slug>:stories - sorted set of stories in a category scored
                     by first_published_date scored by unix time

    tags:<tag_slug>:stories - sorted set of stories tagged with tag_slug
            scored/ordered by first_published_date in unix time

    tags - hash mapping tag slug to tag name

    """

    def __init__(self, publication_short_name, redis, **kwargs):
        self.redis = redis
        self.publication_short_name = publication_short_name
        self.prep_json_fn         = kwargs.get("prep_json_fn", None)
        self.post_save_callbacks  = kwargs.get("post_save_callbacks", [])
        self.finalize_callback    = kwargs.get("finalize_callback", None)
        self.fields               = kwargs.get("fields", [])
        self.aggregate            = kwargs.get("aggregate", [])

    def fetch_stories(self):
        start = 0
        limit = 100
        count = limit
        while count != 0:
            objs = content_objects.filter({
              'type' : 'container',
              'role' : 'story',
            }).offset(start).limit(100).execute()

            stories = map(lambda o: o.toJSONSafe(), objs)
            
            for story in stories:
                print story['slug']
                yield story

            count = len(objs)
            start+=count
            print "Synced", start, "stories"


    def _fetch_publication(self):
        pub = content_objects.filter(
            short_name=self.publication_short_name
        ).execute()[0]

        pub_key = "publication:{}".format(self.publication_short_name)
        self.redis.hmset(
            pub_key,{
                'stored_at': int(datetime.now().strftime("%s")),
                'object'   : pub.toJSON()
            }
        )

    def _fetch_categories(self):
        response = requests.get('http://%s.marquee.by/api/categories/' % "custommade")
        categories = json.loads(response.content)

        pipe = self.redis.pipeline()
        for c in categories:
            pipe.hset("categories", c['_id'].split(":")[1], json.dumps(c))
            pipe.hset("field:category", c['_id'].split(":")[1], c['title'])

        pipe.execute()        

    def sync(self):
        self._fetch_publication()
        self._fetch_categories()
        for story in self.fetch_stories():
            self.store_story(story)

        self.finalize()

    def store_story(self, story):
        story_slug = story['slug']

        story_key = "story:{}".format(story_slug)
        try:
            first_published_date = parser.parse(story['first_published_date'])
        except Exception as e:
            return False

        # This is to handle when the slug, tags, categories, etc change
        old_story_slug = self.redis.hget("id_to_slug", story['id'])
        if old_story_slug:
            self.remove(old_story_slug)

        first_published_date = parser.parse(story['first_published_date'])

        # modify json before storing
        if self.prep_json_fn:
            self.prep_json_fn(story)

        self.redis.zadd(
            "stories",
            **{ story_key : first_published_date.strftime("%s")}
        )

        self.redis.hmset("id_to_slug", {story['id']: story_slug})
        
        self.redis.hmset(story_key, {
            'stored_at' : int(first_published_date.strftime("%s")),
            'object'    : zlib.compress(json.dumps(story), 9)
        })

        self.store_category(story_key, story)
        self.store_tags(story_key, story)

        # extra things to do
        if self.post_save_callbacks:
            for cb in self.post_save_callbacks:
                cb(story_key, story, self.redis)

        return True

    def store_category(self, story_key, story):

        first_published_date = parser.parse(story['first_published_date'])

        category_slug = story.get('category', None)
        if category_slug:
            self.redis.zadd(
                "category:{}:stories".format(category_slug),
                **{story_key : first_published_date.strftime("%s")}
            )
            if "category" in self.aggregate:
                self.redis.sadd(
                    "story:{}:category".format(story['slug']),
                    category_slug
                )

    def store_tags(self, story_key, story):
        tags = story.get('tags', [])
        first_published_date = parser.parse(story['first_published_date'])

        for tag in tags:
            # store <tag_slug> => <tag_name> so we can look up name easily
            self.redis.hset(
                "field:tags",
                tag['slug'],
                tag['name']
            )
            # add story to tag
            self.redis.zadd(
                "tags:{}:stories".format(tag['slug']),
                **{ story_key: first_published_date.strftime("%s")}
            )

            if "tags" in self.aggregate:
                self.redis.zadd("story:{}:tags".format(story['slug']), 1, tag['slug'])
                self.redis.zincrby("aggregate:stories:tags:count", tag['slug'], 1)


    def remove(self, story_slug):
        """
        Public: remove a a story from the cache. It will also
        remove the story from relevant tag and category keys
        """
        story = None
        story_key = "story:{}".format(story_slug)
        story_hash = self.redis.hgetall(story_key)

        if not story_hash:
            return False

        story   = json.loads(zlib.decompress(story_hash['object']))

        story_slug           = story['slug']
        first_published_date = parser.parse(story['first_published_date'])
        category_slug        = story.get('category')
        tags                 = story.get('tags', [])

        if category_slug:
            self.redis.zrem(
                "category:{}:stories".format(category_slug),
                story_key
            )
            if "category" in self.aggregate:
                self.redis.srem(
                    "story:{}:category".format(story['slug']),
                    category_slug
                )
        if tags:
            for tag in tags:
                self.redis.zrem(
                    "tags:{}:stories".format(tag['slug']),
                    story_key
                )
                if "tag" in self.aggregate:
                    self.redis.zrem("story:{}:tags".format(story['slug']), 1, tag['slug'])
                    self.redis.zincrby("aggregate:stories:tags:count", tag['slug'], -1)

            self.redis.delete("story:{}:tags".format(story_slug))


        self.redis.delete(story_key)
        self.redis.zrem("stories", story['slug'])
        return True

    def finalize(self):
        print "finalizing and calling callbacks"
        if self.finalize_callback:
            self.finalize_callback(self.redis)