from app   import settings
from flask import Blueprint

import redis



hyperdrive = Blueprint('hyperdrive', __name__)

if settings.REDIS_URL:
    redisdb = redis.from_url(settings.REDIS_URL)
else:
    redisdb = redis.StrictRedis(host="localhost", port=6379)