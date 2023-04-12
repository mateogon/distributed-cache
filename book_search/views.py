import aiohttp
import logging
import aioredis
import json
from .olids import olids

logger = logging.getLogger(__name__)

REDIS_URL = "redis://redis"


async def get_book_details(olid):
    url = f'https://openlibrary.org/works/{olid}.json'
    timeout = aiohttp.ClientTimeout(total=240)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                print(f"Book not found for OLID {olid}")
                return None

async def get_book_details_cached(redis, olid):
    cache_key = f"book:{olid}"

    cached_data = await redis.get(cache_key)
    if cached_data:
        book_details = json.loads(cached_data)
        return book_details

    book_details = await get_book_details(olid)
    if book_details:
        await redis.set(cache_key, json.dumps(book_details))
    return book_details

async def test_code():
    
    redis = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    logger.info("Starting test()")

    print(olids[23])
    book_details = await get_book_details_cached(redis,olids[23])
    print(book_details)

    logger.info("Finished test()")
