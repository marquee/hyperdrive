from app 			    import settings

from flask 				import current_app
from flask.ext.script   import Manager

from importlib 			import import_module

from .denormalizer import Denormalizer
from .main		   import redisdb

import time

manager = Manager(usage="yo")


@manager.command
def sync_content():
	start_time = time.time()
	print "Initiating hyperdrive sync for %s..." % settings.PUBLICATION_SHORT_NAME
	print "<movie reference>"

	redisdb.flushdb()

	try:
		sync_content = settings.HYPERDRIVE_SETTINGS['SYNC_CONTENT']
		module, fn = settings.HYPERDRIVE_SETTINGS['SYNC_FUNCTION'].rsplit(".", 1)
		sync_content = getattr(import_module(module), fn)
		sync_content()
	except:
		denorm = Denormalizer(
    		settings.PUBLICATION_SHORT_NAME,
    		redisdb
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
	print "Memory:", redisdb.info()['used_memory_human']
	print "Stories:", redisdb.zcard("stories")
	print "Categories:", len(redisdb.hgetall("categories"))
	print "Tags:", len(redisdb.hgetall("field:tags"))
	
	