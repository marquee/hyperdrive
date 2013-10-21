from app 			    import settings

from flask 				import current_app
from flask.ext.script   import Manager

import time
import redis

from hyperdrive.denormalizer import Denormalizer

if settings.REDIS_URL:
    db = redis.from_url(settings.REDIS_URL)
else:
    db = redis.StrictRedis(host="localhost", port=6379)

manager = Manager(usage="yo")

@manager.command
def sync_content():
	start_time = time.time()
	print "Initiating hyperdrive sync for %s..." % settings.PUBLICATION_SHORT_NAME
	print "<movie reference>"

	db.flushdb()
	denorm = Denormalizer(
    	settings.PUBLICATION_SHORT_NAME,
    	db
	)

	denorm.sync()
	end_time  = time.time()

	print "---------------------------------------"
	print "Time Elapsed: ", end_time - start_time
	print "======================================="
	stats()


@manager.command
def stats():
	print "Content Stats"
	print "---------------------------------------"
	print "Memory:", db.info()['used_memory_human']
	print "Stories:", db.zcard("stories")
	print "Categories:", len(db.hgetall("categories"))
	print "Tags:", len(db.hgetall("field:tags"))
	
	