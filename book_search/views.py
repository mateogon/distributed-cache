import aiohttp
import logging
import aioredis
import json
import hashlib
import numpy as np
import asyncio
import sys
from .olids import olids
from .redis_instances import redis_instances
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor()
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
'''
async def get_book_details_cached(redis, olid):
    cache_key = f"book:{olid}"

    cached_data = await redis.get(cache_key)
    if cached_data:
        book_details = json.loads(cached_data)
        return book_details, True

    book_details = await get_book_details(olid)
    if book_details:
        await redis.set(cache_key, json.dumps(book_details))
    return book_details, False
'''
async def get_book_details_cached(redis, olid):
    cache_key = f"book:{olid}"

    cached_data = await redis.get(cache_key)
    if cached_data:
        book_details = json.loads(cached_data)
        return book_details, True

    book_details = await get_book_details(olid)
    if book_details:
        # Check if the key count is over the limit (e.g., 100)
        key_count = await redis.dbsize()
        print(f"Key count: {key_count}")
        if key_count >= 10:
            print("Key count is over the limit. Evicting the least recently used key.")
            # Apply LRU eviction policy by finding the least recently used key
            keys = await redis.keys("*")
            least_recently_used_key = None
            min_ttl = float("inf")

            for key in keys:
                ttl = await redis.ttl(key)
                print(f"Key: {key}, TTL: {ttl}")
                if ttl < min_ttl:
                    print(f"Key {key} has the least TTL: {ttl}")
                    min_ttl = ttl
                    least_recently_used_key = key

            # Remove the least recently used key from the cache
            if least_recently_used_key:
                await redis.delete(least_recently_used_key)

        # Add the new key to the cache
        await redis.set(cache_key, json.dumps(book_details), ex=3600)  # Set the TTL as required

    return book_details, False

def get_redis_instance(olid, redis_instances):
    olid_hash = hashlib.md5(olid.encode('utf-8')).hexdigest()
    instance_index = int(olid_hash, 16) % len(redis_instances)
    print(f"OLID {olid} goes to instance {instance_index}")
    return redis_instances[instance_index]

async def query_book_details(olid, redis_instances):
    logger.info(f"Querying book details for OLID {olid}")
    target_redis = get_redis_instance(olid, redis_instances)
    
    book_details, cache_hit = await get_book_details_cached(target_redis, olid)
    logger.info(f"Book details for OLID {olid} retrieved from cache: {cache_hit}")
    return cache_hit

async def reset_cache(redis_instances):
    tasks = [redis.flushall() for redis in redis_instances]
    await asyncio.gather(*tasks)

async def get_key_count(redis_instance):
        return await redis_instance.dbsize()

async def get_total_key_count(redis_instances):
    key_counts = await asyncio.gather(*[get_key_count(redis_instance) for redis_instance in redis_instances])
    return sum(key_counts)

async def async_request(query_function, *args):
    return await query_function(*args)


def sync_request(query_function, *args):
    loop = asyncio.get_event_loop()
    coro = query_function(*args)
    return asyncio.run_coroutine_threadsafe(coro, loop).result()

async def test_code(distribution='pareto', num_queries=40, use_async=True):
    test_queries = True
    should_reset_cache = False

    request = async_request if use_async else sync_request

    logger.info("Starting test()")

    if should_reset_cache:
        logger.info("Resetting cache")
        await reset_cache(redis_instances)
        total_key_count = await get_total_key_count(redis_instances)
        print(f"Total number of stored cache keys across all Redis instances after reset: {total_key_count}")
        return

    if test_queries: # test for queries
        pareto_shape = 2
        exponential_scale = 50
        custom_distribution = np.random.normal

        distributions = {
            'pareto': lambda size: np.random.pareto(pareto_shape, size),
            'exponential': lambda size: np.random.exponential(exponential_scale, size),
            'even': lambda size: np.random.uniform(0, 1, size),
            'custom': lambda size: custom_distribution(0, 1, size)
        }

        if distribution not in distributions:
            raise ValueError(f"Invalid distribution: {distribution}. Available options: {', '.join(distributions.keys())}")

        # Generate random numbers according to the specified distribution for all OLIDs
        probabilities = distributions[distribution](len(olids))
        normalized_probabilities = probabilities / probabilities.sum()

        # Sample OLIDs using the generated probabilities
        olids_sample = np.random.choice(olids, num_queries, p=normalized_probabilities)

        
        logger.info(f"Querying {num_queries} books")
        tasks = [request(query_book_details, olid, redis_instances) for olid in olids_sample]
        logger.info(f"Waiting for {len(tasks)} tasks to complete")
        if use_async:
            cache_hits_list = await asyncio.gather(*tasks)
        else:
            with ThreadPoolExecutor() as executor:
                cache_hits_list = list(executor.map(request, tasks))
        cache_hits = sum(cache_hits_list)

        cache_hit_rate = cache_hits / num_queries
        logger.info(f"Cache hit rate: {cache_hit_rate:.2%}")
        
    else: #test for memory requirements
        
        async def get_sample_responses(sample_size):
            responses = await asyncio.gather(*[get_book_details(olid) for olid in olids[:sample_size]])
            return responses
        
        def format_memory_size(size_in_bytes):
            units = ['B', 'KB', 'MB', 'GB', 'TB']
            index = 0

            while size_in_bytes >= 1024 and index < len(units) - 1:
                size_in_bytes /= 1024
                index += 1

            return f"{size_in_bytes:.2f} {units[index]}"



        # Choose a sample size, e.g., 20
        sample_size = 1000
        if use_async:
            sample_responses = await get_sample_responses(sample_size)
        else:
            with ThreadPoolExecutor() as executor:
                sample_responses = executor.submit(sync_request, get_sample_responses, sample_size).result()
        response_sizes = [sys.getsizeof(json.dumps(response)) for response in sample_responses]
        
        average_response_size = sum(response_sizes) / len(response_sizes)
        num_responses = 100  # The number of API responses you want to store
        buffer_factor = 1.2  # Add a 20% buffer for Redis memory overhead

        memory_required = num_responses * average_response_size * buffer_factor
        formatted_memory_required = format_memory_size(memory_required)
        logger.info(f"Estimated memory required for {sample_size} queries: {formatted_memory_required}")
    
    total_key_count = await get_total_key_count(redis_instances)
    print(f"Total number of stored cache keys across all Redis instances after reset: {total_key_count}")
        
    logger.info("Finished test()")
