# Hyperdrive
Hyperdrive is a plugin for the [Marquee Runtime](https://github.com/marquee/runtime) to store a local, query optimized, copy of the content in [Redis](http://redis.io/) so app is fast and in sync with the content api. This is accomplished through denormalizing the data, precomputing queries, and listening to web hooks from marquee. 

It is recommended you use the marquee runtime's [data loader](https://github.com/marquee/runtime/blob/master/app/data_loader.py) to build the app and once you know what queries you need, configure hyperdrive for what you need. 

## Denormalizer
`Denormalizer` is an object that handles synchronizing the content api with the redis data. You should use this to create a script to do the initial content sync as well as a webhook to stay in sync with updates.

**NOTE:** Eventually this will be abstracted to all types of container object roles in the content api. 

**Arguments:**
* `histograms` - A list of strings representing fields that we want to compute histograms on.
* `redis` - the redis object
* `prep_json_funcs` - (optional) A list of functions that will be called before the json is stored. Will be passed the json object.
* `post_save_funcs` - (optional) A list of functions to call after a story is saved. They will be passed the story key, story object and the `redis` object. 
* `finalalize_funcs` - (optional) A list of functions to call during the finalization step. They will be passed the `redis` object.

**Example:**
If you know you'll have to do query story objects by `category` or `byline`, then set them to initialize the denormalizer like this:

    denorm = Denormalizer(
      fields = ["category", "tags"],
			histograms = ["tags"],
			redis = r,
    )
	
This will create keys for each category and tag value mapped to a sorted set of stories. 

## StorySet
StorySet is an interface to retrieve story objects stored in redis sorted sets. At the moment, stories are sorted by `first_published_date` but there will be a way to specify sorting in the future.

### Methods for StorySet

**select**
    StorySet.select(**kwargs)
Class method that returns a `StorySet` instance representing a set of stories that match query parameters.

This example selects 10 most recent stories in the history category that aren't tagged "medieval" or "renaissance":
	StorySet.select(tags__nin=["medieval", "renaissance"], category="history")[0:10]

**histogram**
    self.histogram(field, n=10)
Instance method that returns a list of dictionaries with frequencies.
    StorySet().histogram("tags", n=10)
    # => [{'name':'US History','slug':'us-history', {'count':30}, …

**map**
    self.map(model_class)
Instance method that wraps the stories in the set to model class. 
*TODO:* Need to rethink this. Slightly of awkward/crappy. 

**all**
    StorySet.all()
Class method that returns a set with all stories. 

### Available operators:

**ne**
Selects sets not equal to the value

**in**
Selects sets in a given list
Example:
	StorySet.select(tags__in=["action", "comedy"]) 

**nin**
Excludes sets in the given list from the set selection.
Example:
	StorySet.select(tags__nin=["boring", "dull"])

### TODO: write about set operations