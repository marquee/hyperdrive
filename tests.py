from content        import ContentObjects, Container
from queryset       import StorySet
from denormalizer   import Denormalizer
from models         import *
import unittest
import redis
import json
import zlib

import os
from app import settings

content_objects = ContentObjects(
    settings.CONTENT_API_TOKEN,
    api_root = settings.CONTENT_API_ROOT,
)


class SetOperationsTests(unittest.TestCase):

    def setUp(self):
        self.redis = redis.StrictRedis(host='localhost', port=6379, db=1)

        self.redis.flushdb()

        

# class StorySetTests(unittest.TestCase):

#     def setUp(self):
#         print "ASDFAS"
#         self.redis = redis.StrictRedis(host='localhost', port=6379, db=1)
        # self.redis.flushdb()

        # self.sample_story = json.load(open('hyperdrive/sample_story.json'))

        # sample_story = self.sample_story
    
        # category = sample_story['category']
        # tags     = sample_story['tags']

        # story_map = {
        #     "type"      : "story",
        #     "id"        : "slug",
        #     "fields"    : ["category", "tags"],
        #     # "sort" : {
        #     #     "field": "first_published_date",
        #     #     "type" : "datetime", # needs to be a type we can get a int/float for because sorted set
        #     # },
        # }

        # denormalizer = Denormalizer(story_map, redis=self.redis)

        # # denormalizer.denormalize_story(sample_story)  

        # self.denormalizer = denormalizer


        # self.story_slug  = self.sample_story['slug']
        # self.story_key   = "story:{}".format(self.sample_story['slug'])

        # import time

        # self.objs = []
        # start = 50
        # limit = 100
        # while True:
        #     print "At: ", start
        #     try:
        #         objs = content_objects.filter({
        #           'type'                 : 'container',
        #           'role'                 : 'story',          
        #         }).offset(start).limit(limit).execute()
        #     except:
        #         objs = None

        #     if objs:
        #         self.objs += objs
        #     else:
        #         break
        #     start+=limit

        # # # self.objs = content_objects.filter({
        # # #   'type'                 : 'container',
        # # #   'role'                 : 'story',          
        # # # }).offset(50).limit(50).execute()      

        # print "COUNT: ", len(self.objs)  
        
        # denormalizer.proccess(
        #     [o.toJSONSafe() for o in self.objs]
        # )

        # self.denormalizer = denormalizer


    # def print_tags(self):
    #     story_tag_keys = map("story:{}:tags".format, self.redis.zrange("stories", 0,-1))
    #     self.redis.zunionstore("tag_counts", story_tag_keys)
    #     tag_counts = []
    #     for t in self.redis.zrevrange("tag_counts", 0, 100, withscores=True):
    #         tag_name = self.redis.hget("tags", t[0])
    #         print tag_name, t[1]            

    # def testSelect(self):
    #     self.print_tags()
    #     print StorySet.select(
    #         tags__nin=["new-york-city"],
    #         category    = "video"
    #     )


#     def testUnion(self):
#         press_stories = self.redis.zrange("tags:press:stories", 0, -1)
#         house_stories = self.redis.zrange("category:house:stories", 0, -1)

#         press_or_house_stories = press_stories | house_stories
#         self.assertEquals(
#             len(press_or_house_stories),
#             len(StorySet(tags="press") + StorySet(category="house"))
#         )

#     def testIntersection(self):
#         self.objs = content_objects.filter({
#           'type'                 : 'container',
#           'role'                 : 'story',
#           # 'category__in'             : ['jewelry', 'house']
#         }).offset(800).limit(100).execute()

#         self.denormalizer.proccess(
#             [o.toJSONSafe() for o in self.objs]
#         )        
#         custom_stories = self.redis.zrange("tags:custom:stories", 0, -1)
#         jewelry_stories = self.redis.zrange("category:jewelry:stories", 0, -1)

#         jewelry_AND_custom_stories = set(jewelry_stories) & set(custom_stories) 


#         print len(set(jewelry_stories)), len(set(custom_stories) )
#         print len(set(jewelry_stories) & set(custom_stories))

#         print len(set(custom_stories) - set(jewelry_stories))

#         self.assertEquals(
#             len(jewelry_AND_custom_stories),
#             len(StorySet(tags="custom") & StorySet(category="jewelry"))
#         )

#     def testDifference(self):
#         self.objs = content_objects.filter({
#           'type'                 : 'container',
#           'role'                 : 'story',
#         }).offset(800).limit(100).execute()

#         self.denormalizer.proccess(
#             [o.toJSONSafe() for o in self.objs]
#         )        
#         custom_stories = self.redis.zrange("tags:custom:stories", 0, -1)
#         jewelry_stories = self.redis.zrange("category:jewelry:stories", 0, -1)

#         # jewelry_AND_custom_stories = 

#         print len(set(jewelry_stories)), len(set(custom_stories))
#         print len(set(jewelry_stories) - set(custom_stories))

#         jewelry_minus_custom_stories = set(jewelry_stories) - set(custom_stories)
#         self.assertEquals(
#             len(jewelry_minus_custom_stories),
#             len(StorySet(category="jewelry") - StorySet(tags="custom"))
#         )

#         custom_minus_jewelry_stories = set(custom_stories) - set(jewelry_stories)
#         self.assertEquals(
#             len(custom_minus_jewelry_stories),
#             len(StorySet(tags="custom") - StorySet(category="jewelry"))
#         )

#     def testSlice(self):

#         self.assertEquals(
#             len(StorySet(setkey="stories")[1:3]),
#             2
#         )

# class ModelTests(unittest.TestCase):
#     def setUp(self):
#         self.redis = redis.StrictRedis(host='localhost', port=6379, db=1)
#         self.redis.flushdb()

#         self.sample_story = json.load(open('app/sample_story.json'))

#         sample_story = self.sample_story
    
#         category = sample_story['category']
#         tags     = sample_story['tags']

#         story_map = {
#             "type"      : "story",
#             "id"        : "slug",
#             "fields"    : ["category"],
#             # "sort" : {
#             #     "field": "first_published_date",
#             #     "type" : "datetime", # needs to be a type we can get a int/float for because sorted set
#             # },            
#         }        

#         denormalizer = Denormalizer(story_map, redis=self.redis)

#         denormalizer.denormalize_story(sample_story)




# class DenormalizerTests(unittest.TestCase):

#     def setUp(self):
#         self.redis = redis.StrictRedis(host='localhost', port=6379, db=1)
#         self.redis.flushdb()

#         self.sample_story = json.load(open('hyperdrive/sample_story.json'))

#         sample_story = self.sample_story
    
#         category = sample_story['category']
#         tags     = sample_story['tags']

#         story_map = {
#             "type"      : "story",
#             "id"        : "slug",
#             "object_set": "stories",
#             "fields"    : ["category"],
#             # "sort" : {
#             #     "field": "first_published_date",
#             #     "type" : "datetime", # needs to be a type we can get a int/float for because sorted set
#             # },            
#         }        

#         denormalizer = Denormalizer(story_map, redis=self.redis)

#         denormalizer.store_story(sample_story)  

#         self.denormalizer = denormalizer

#         self.story_slug  = self.sample_story['slug']
#         self.story_key   = "story:{}".format(self.sample_story['slug'])


#     def testStoryKey(self):
#         """
#         Public: Tests that story key exists and has correct content
#         """

        
#         story_hash = self.redis.hgetall(self.story_key)

#         story = json.loads(zlib.decompress(story_hash['object'])) 
#         self.assertTrue(
#             story == self.sample_story
#         )

#         self.denormalizer.remove(self.story_slug)
#         self.assertFalse(
#             self.redis.exists(self.story_key)
#         )        

# #     def testCategoryStories(self):
# #         category_stories = "category:{}:stories".format(self.sample_story['category'])

# #         # Check that the story is in correct the category story set
# #         self.assertTrue(
# #             self.story_key in self.redis.zrange(category_stories, 0, -1)
# #         )

# #         self.denormalizer.remove(self.sample_story['slug'])
# #         # test removal
# #         self.assertFalse(
# #             self.story_key in self.redis.zrange(category_stories, 0, -1)
# #         )        

# #     def testTagStories(self):
# #         for tag in self.sample_story['tags']:
# #             tag_stories      = "tags:{}:stories".format(tag['slug'])
# #             self.assertTrue(
# #                 self.story_key in self.redis.zrange(tag_stories, 0, -1)
# #             )

# #         # test removal
# #         self.denormalizer.remove(self.sample_story['slug'])
# #         for tag in self.sample_story['tags']:
# #             tag_stories      = "tags:{}:stories".format(tag['slug'])
# #             self.assertFalse(
# #                 self.story_key in self.redis.zrange(tag_stories, 0, -1)
# #             )


if __name__ == '__main__':
    unittest.main() 