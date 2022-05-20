import redis

from os import environ as env

REDIS_PORT=env.get("REDIS_PORT")
REDIS_HOST=env.get("REDIS_HOST")
REDIS_PASS=env.get("REDIS_PASS")

def connect_redis():
    return redis.Redis(password=REDIS_PASS, host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

