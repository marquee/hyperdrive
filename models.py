from content        import Container
from storyset       import StorySet
from .main          import redisdb as redis_db
from app import settings
import json, zlib
import redis


class ROLES(object):
    STORY       = 'story'
    PUBLICATION = 'publication'
    ISSUE       = 'issue'
    CATEGORY    = 'category'



class MContentModel(object):
    """
    Internal: A base class that wraps the Container content object, providing
    accessors directly to the Container instance as well as some helper
    methods.
    """
    def __init__(self, container):
        self._container = container

    def __getattr__(self, attr_name):
        return getattr(self._container, attr_name)

    def __getattr__(self, attr_name):
        return getattr(self._container, attr_name)

    @property
    def link(self):
        return '/{0}/'.format(self.slug)



class HasCoverContent(object):
    """
    Private: mixin that adds cover content accessors.
    """

    def is_low_res_cover(self):
        """
        Public: if the cover is a gallery, cycles through to see if it container
        low resolution images.

        Returns True if there is an image in the gallery under 800px, False otherwise
        """

        if hasattr(self, "cover_content") and isinstance(self.cover_content, list) and len(self.cover_content) > 0:
            for img_obj in self.cover_content:
                if img_obj['original']['width'] < 800:
                    return True

            return False


    def cover(self, size='640', as_obj=False):
        """
        Public: return the URL for the specified image size, or '' if the
        object doesn't have a cover_content image of that size. If the cover
        is a gallery or embed type, returns the first image or fallback image,
        respectively.

        size    - (optional: '640') The int or str size to select.
        as_obj  - (optional: False) If True, returns the whole image object.

        Returns the string URL of the image.
        """

        default = ''

        if not hasattr(self, 'cover_content'):
            return default

        asset = None
        image_obj = None

        if isinstance(self.cover_content, list) and len(self.cover_content) > 0:
            image_obj = self.cover_content[0]
        elif isinstance(self.cover_content, dict) and 'image' in self.cover_content:
            image_obj = self.cover_content['image']
        else:
            image_obj = self.cover_content


        if image_obj:
            size = str(size)
            if size == '640':
                asset = image_obj['content'].get('640', {})

            elif size == '1280':
                asset = image_obj['content'].get('1280', {})

            elif size == 'original':
                asset = image_obj.get('original', {})

            if asset:
                if as_obj:
                    return asset
                return asset.get('url', default)
        return default



class Issue(MContentModel, HasCoverContent):
    """
    Public: A model that corresponds to a Container with role='issue'.
    """

    def stories(self, *args, **kwargs):
        """
        Public: returns all stories that belong to the Issue

        Returns an APIQuery containing instances of Story objects for
        every story in an issue.
        """

        return StorySet.select(issue=self.slug)

class Category(object):

    def __init__(self, category_dict):
        self.slug   = category_dict['slug']
        self.title  = category_dict['title']


    def stories(self, *args, **kwargs):
        stories = content_objects.filter(
            category__slug=self.slug
        ).mapOnExecute(Story)

        return stories


class Story(MContentModel, HasCoverContent):
    """
    Public: A model that corresponds to a Container with role='story'.
    """

    @property
    def published(self):
        """
        Public: map the published data to .published.

        Right now, uses the `_include_published` flag on the query. However,
        it may change to be a query like `?_as_of=@published_date`, so this
        abstraction will keep the template API consistent.

        Returns a Story copy of the story instance in its published state, or
        None if the instance is not published.
        """
        if self._container._published_json:
            return Story(Container(self._container._published_json[0]))
        return None

    def keywords(self):
        """
        Public: return space-separated tags for use in meta keywords tag

        Returns a string
        """
        keywords = ''
        for t in getattr(self, "tags", []):
            keywords += u"{0} ".format(t['name'])
        keywords.strip()
        return keywords

    def related_stories(self, *args, **kwargs):
        """
        Public: returns stories marked as related to the current Story

        Returns an APIQuery containing instance of Story objects.
        """

        related_stories = content_objects.filter(
            related_content=self.id
        ).mapOnExecute(Story)

        return related_stories

    @property
    def link(self):
        try:
            return settings.STORY_URL(self)
        except AttributeError:
            return "/{1}/".format(self.slug)

class Publication(MContentModel):
    """
    Public: A model that corresponds to a Container with `role='publication'`.
    """

    def __init__(self):
        pub_key = "publication:{}".format(settings.PUBLICATION_SHORT_NAME)

        publication_container = Container(json.loads(
            redis_db.hgetall(pub_key)['object']
        ))

        self._container = publication_container

    def issues(self, *args, **kwargs):
        """
        Public: load the Issues that belong to the Publication instance from
        the API, filtered by the specified arguments.

        args    - (optional) A single dict to use for the query, allowing for
                    query keys that cannot be used as keywoard arguments.

        kwargs  - (optional) Keyword arguments that are added to the query,
                    superseding any query specified as a positional argument.

        Note: the query is updated to filter by role and to only include
        published stories.

        Returns an (iterable, lazy) APIQuery of Story objects.
        """

        issue_content_map = redis_db.hgetall("issue_content")

        issue_keys = redis_db.zrevrange("issues", 0, -1)

        issues =  [
            issue_content_map[issue_key.split(":")[-1]]  for issue_key in issue_keys
        ]

        issue_wrapper = lambda issue: Issue(Container(json.loads(issue)))
        return map(issue_wrapper, issues)

    def categories(self):
        cs = []
        for cat,val in redis_db.hgetall("category_content").items():
            cs.append(json.loads(val))
        return cs

    def get_category(self, slug):
        try:
            return json.loads(redis_db.hget("category_content", slug))
        except:
            return None

    def get_issue(self, slug):
        obj = json.loads(redis_db.hget("issue_content", slug))
        issue_container = Container(obj)
        return Issue(issue_container)

    def stories(self, **kwargs):
        """
        Public: Convenience method to select a storyset.

        kwargs  - (optional) Keyword arguments passed to the StorySet.select method

        Returns an iterable StorySet object.
        """
        return StorySet.select(**kwargs)



def modelFromRole(content_obj):
    """
    Public: convert a Content object to the appropriate Marquee model.

    content_obj - the Container to wrap in the model.

    Returns an Issue, Story, or Publication instance.
    """
    mapping = {
        ROLES.ISSUE         : Issue,
        ROLES.STORY         : Story,
        ROLES.PUBLICATION   : Publication,
    }
    return mapping[content_obj.role](content_obj)
