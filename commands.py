from app                import settings
from app.utils          import import_by_path

from flask              import current_app
from flask.ext.script   import Manager

from .denormalizer import Denormalizer
from .main         import redisdb

import time

manager = Manager(usage="yo")

@manager.option("--destructive")
def sync_content(destructive=False):
    start_time = time.time()
    print "Initiating hyperdrive sync for %s..." % settings.PUBLICATION_SHORT_NAME
    print "<movie reference>"

    

    if destructive == "true":
        redisdb.flushdb()    
    else:
        print "Starting non-destructive sync"
        
    try:
        sync_content = import_by_path(settings.HYPERDRIVE_SETTINGS['SYNC_FUNCTION'])
        sync_content()
    except ImportError:
        print "IMPORT ERROR"
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