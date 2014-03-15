from .denormalizer  import Denormalizer
from .main          import hyperdrive, redisdb
from app            import settings

from flask          import abort, request

import json
import redis

from app import settings
from app.utils import import_by_path

def handle_issue_event(action, issue, denorm):
    if action == "publish":
        denorm.store_issue(issue)
    elif action in ["unpublish", "delete"]:
        denorm.remove_issue(issue)

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

    try:
        DenormalizerClass = import_by_path(settings.HYPERDRIVE_SETTINGS['DENORMALIZER'])
    except Exception as e:
        print e
        DenormalizerClass = Denormalizer

    denorm = DenormalizerClass(
        settings.PUBLICATION_SHORT_NAME,
        redisdb
    )
    
    if request.method == "POST":
        data = json.loads(request.data)
        if data.get('role', None) == "issue":
            handle_issue_event(data['action'], data['issue'], denorm)

            return json.dumps({
                "status" : "good"
            })

        else:
            action  = data['action']
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
