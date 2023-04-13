import aioredis

redis_instances = [
    aioredis.from_url("redis://redis1", encoding="utf-8", decode_responses=True),
    aioredis.from_url("redis://redis2", encoding="utf-8", decode_responses=True),
    aioredis.from_url("redis://redis3", encoding="utf-8", decode_responses=True),
]
