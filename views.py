from .denormalizer  import Denormalizer
from .main          import hyperdrive, redisdb
from app            import settings

from flask          import abort, request

import json
import redis

@hyperdrive.route("/thisisareallyobscureurltoawebhook/", methods=["POST"])
def webhook():
    """
    Public: Webhook to recieve publish and unpublish events from the content api
    so we can keep redis in sync

    The payload looks like
        action - [publish, unpublish]
        story_json - json string of story obj(if action == publish)
        story_slug - slug of story to be removed(if action == unpublish)
    """

    denorm = Denormalizer(
        settings.PUBLICATION_SHORT_NAME,
        redisdb
    )
    

    if request.method == "POST":

        data    = json.loads(request.data)
        action  = data['action']
        # print "YOYOYOYO", data
        if action == "publish":
            story = json.loads(data['story_json'])
            success = denorm.store_story(story)

        elif action in ["unpublish", "delete"]:
            story_slug = data['story_slug']
            success = denorm.remove(story_slug)

        status = "good" if success else "bad"

        return json.dumps({
            'status' : status
        })

    abort(405)
