import aiohttp
import random
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
from django.http import HttpResponse
import timeit
import matplotlib.pyplot as plt

executor = ThreadPoolExecutor()
logger = logging.getLogger(__name__)

cache_policy = "random"
max_cache_keys = 40 #per instance
pareto_shape = 1.16
exponential_scale = 50

async def book_details_view(request, olid):
    target_redis = get_redis_instance(olid, redis_instances)
    book_details, _ = await get_book_details_cached(target_redis, olid, cache_policy)

    if book_details:
        response_data = json.dumps(book_details)
        return HttpResponse(response_data, content_type='application/json')
    else:
        response_data = json.dumps({"error": "Book details not found."})
        return HttpResponse(response_data, content_type='application/json', status=404)


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
            
async def cache_eviction_policy(redis, policy="lru"):
    if policy == "lru":
        return await lru_eviction(redis)
    elif policy == "fifo":
        return await fifo_eviction(redis)
    elif policy == "lfu":
        return await lfu_eviction(redis)
    elif policy == "random":
        return await random_eviction(redis)
    else:
        raise ValueError("Unsupported cache eviction policy")

async def lfu_eviction(redis):
    keys = await redis.keys("*")
    least_frequently_used_key = None
    min_frequency = float("inf")

    for key in keys:
        frequency = int(await redis.object("freq", key))
        if frequency < min_frequency:
            min_frequency = frequency
            least_frequently_used_key = key

    if least_frequently_used_key:
        logger.info(f"Evicting key {least_frequently_used_key} from cache (LFU)")
        await redis.delete(least_frequently_used_key)

async def lru_eviction(redis):
    keys = await redis.keys("*")
    least_recently_used_key = None
    min_ttl = float("inf")

    for key in keys:
        ttl = await redis.ttl(key)
        if ttl < min_ttl:
            min_ttl = ttl
            least_recently_used_key = key

    if least_recently_used_key:
        logger.info(f"Evicting key {least_recently_used_key} from cache (LRU)")
        await redis.delete(least_recently_used_key)


async def fifo_eviction(redis):
    keys = await redis.keys("*")
    oldest_key = None
    max_ttl = -float("inf")

    for key in keys:
        ttl = await redis.ttl(key)
        if ttl > max_ttl:
            max_ttl = ttl
            oldest_key = key

    if oldest_key:
        logger.info(f"Evicting key {oldest_key} from cache (FIFO)")
        await redis.delete(oldest_key)

async def random_eviction(redis):
    keys = await redis.keys("*")

    if keys:
        random_key = random.choice(keys)
        logger.info(f"Evicting key {random_key} from cache (Random)")
        await redis.delete(random_key)

async def get_book_details_cached(redis, olid, cache_policy="lru"):
    cache_key = f"book:{olid}"

    cached_data = await redis.get(cache_key)

    if cached_data:
        book_details = json.loads(cached_data)
        return book_details, True

    book_details = await get_book_details(olid)

    if book_details:
        key_count = await redis.dbsize()

        if key_count >= max_cache_keys:
            print("Key count is over the limit. Evicting based on cache policy.")
            await cache_eviction_policy(redis, policy=cache_policy)

        await redis.set(cache_key, json.dumps(book_details), ex=3600)

    return book_details, False


def get_redis_instance(olid, redis_instances):
    
    olid_hash = hashlib.md5(olid.encode('utf-8')).hexdigest()
    instance_index = int(olid_hash, 16) % len(redis_instances)
    logger.info(f"OLID {olid} goes to instance {instance_index}")
    return redis_instances[instance_index]

async def query_book_details(olid, redis_instances, cache_policy="lru"):
    
    target_redis = get_redis_instance(olid, redis_instances)
    book_details, cache_hit = await get_book_details_cached(target_redis, olid, cache_policy)
    logger.info(f"Book details for OLID {olid} retrieved from cache: {cache_hit}")
    return cache_hit

async def reset_cache():
    
    tasks = [redis.flushall() for redis in redis_instances]
    await asyncio.gather(*tasks)
    await log_total_key_count(redis_instances)
    
async def get_key_count(redis_instance):
        return await redis_instance.dbsize()

async def get_total_key_count(redis_instances):
    key_counts = await asyncio.gather(*[get_key_count(redis_instance) for redis_instance in redis_instances])
    for i, count in enumerate(key_counts):
        logger.info(f"Instance {i} key count: {count}")
    return sum(key_counts)

async def log_total_key_count(redis_instances):
    total_key_count = await get_total_key_count(redis_instances)
    logger.info(f"Total key count across all Redis instances: {total_key_count}")
    
def generate_samples(distribution, olids, num_queries,pareto_shape = 1.16,exponential_scale = 50):
    
    
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
    
    return olids_sample

async def estimate_memory_size():
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
    sample_responses = await get_sample_responses(sample_size)
    response_sizes = [sys.getsizeof(json.dumps(response)) for response in sample_responses]
    
    average_response_size = sum(response_sizes) / len(response_sizes)
    num_responses = 100  # The number of API responses you want to store
    buffer_factor = 1.2  # Add a 20% buffer for Redis memory overhead

    memory_required = num_responses * average_response_size * buffer_factor
    formatted_memory_required = format_memory_size(memory_required)
    logger.info(f"Estimated memory required for {sample_size} queries: {formatted_memory_required}")



async def test_code_async(distribution='pareto',cache_policy = "lru", num_queries=300):
    
    test_queries = True
    
    should_reset_cache = False
 
    logger.info("Starting test()")
    
    if should_reset_cache:
        logger.info("Resetting cache")
        await reset_cache(redis_instances)
        await log_total_key_count(redis_instances)

        return
    
    if test_queries: # test for queries
        logger.info("Testing queries")
        
        olids_sample = generate_samples(distribution, olids, num_queries)

        tasks = [query_book_details(olid, redis_instances,cache_policy = cache_policy) for olid in olids_sample]
        cache_hits_list = await asyncio.gather(*tasks)
        cache_hits = sum(cache_hits_list)

        cache_hit_rate = cache_hits / num_queries
        logger.info(f"Cache hit rate: {cache_hit_rate:.2%}")
        
    else: #test for memory requirements
        logger.info("Testing memory requirements")
        estimate_memory_size()

    await log_total_key_count(redis_instances)
        
    logger.info("Finished test()")

async def test_code_sync(distribution='pareto',cache_policy = "lru", num_queries=100):
    
    test_queries = True
    
    should_reset_cache = False
 
    logger.info("Starting test()")
    
    if should_reset_cache:
        logger.info("Resetting cache")
        reset_cache()
        return
    
    if test_queries: # test for queries
        logger.info("Testing queries")
        
        olids_sample = generate_samples(distribution, olids, num_queries)

        cache_hits = 0
        for olid in olids_sample:
            cache_hit = await query_book_details(olid, redis_instances, cache_policy = cache_policy)
            cache_hits += cache_hit

        cache_hit_rate = cache_hits / num_queries
        logger.info(f"Cache hit rate: {cache_hit_rate:.2%}")
        
    else: #test for memory requirements
        logger.info("Testing memory requirements")
        estimate_memory_size()

        
    await log_total_key_count(redis_instances)
        
    logger.info("Finished test()")
    
async def performance_test(distribution='pareto',cache_policy="lru", num_queries=100, test_cached=True, test_non_cached=True, pareto_shape = 1.16,exponential_scale = 50):
    
    olids_sample = generate_samples(distribution, olids, num_queries,pareto_shape,exponential_scale)

    # Measure the execution time of cached queries
    count = 0
    cache_hits = 0
    if test_cached:
        start_time = timeit.default_timer()
        for olid in olids_sample:
            cache_hit = await query_book_details(olid, redis_instances,cache_policy)
            cache_hits += cache_hit
            count += 1
            logger.info(f"cached test: {count}/{num_queries}")
        cached_time = timeit.default_timer() - start_time
    else:
        cached_time = None
        cache_hit_rate = None

    count = 0
    non_cached_times = []
    # Measure the execution time of non-cached queries
    if test_non_cached:
        start_time = timeit.default_timer()
        for olid in olids_sample:
            target_redis = get_redis_instance(olid, redis_instances) # because cached queries already did this
            this_start_time = timeit.default_timer()
            cache_hit = await get_book_details(olid)
            execution_time = timeit.default_timer() - this_start_time
            non_cached_times.append(execution_time)
            count += 1
            logger.info(f"non cached test: {count}/{num_queries}")
        non_cached_total_time = timeit.default_timer() - start_time
        non_cached_avg_time = np.mean(non_cached_times)
        non_cached_std = np.std(non_cached_times)
        print(f'non_cached_avg_time: {non_cached_avg_time} non_cached_std: {non_cached_std}')
    else:
        non_cached_total_time = None
        non_cached_avg_time = None
        non_cached_std = None
    
    if test_cached:
        cache_hit_rate = cache_hits / num_queries
        print(f"Cache hit rate: {cache_hit_rate:.2%}")
        
    return cached_time,non_cached_total_time, cache_hit_rate

async def plot_test():
    num_queries = 10
    non_cached_time = num_queries * 0.843225 #await performance_test(num_queries=num_queries, test_non_cached=True, test_cached=False, pareto_shape=pareto_shapes[0])
    pareto_shapes = [1.16, 1.5, 2, 2.5]
    cached_time  , _ , hit_rate  = await performance_test(num_queries=num_queries,cache_policy = cache_policy, test_non_cached=False, pareto_shape=pareto_shapes[0])
    cached_time1 , _ , hit_rate1 = await performance_test(num_queries=num_queries,cache_policy = cache_policy, test_non_cached=False, pareto_shape=pareto_shapes[1])
    cached_time2 , _ , hit_rate2 = await performance_test(num_queries=num_queries,cache_policy = cache_policy, test_non_cached=False, pareto_shape=pareto_shapes[2])
    cached_time3 , _ , hit_rate3 = await performance_test(num_queries=num_queries,cache_policy = cache_policy, test_non_cached=False, pareto_shape=pareto_shapes[3])

    bar_labels = [f'Cached {pareto_shapes[0]}', f'Cached {pareto_shapes[1]}', f'Cached {pareto_shapes[2]}', f'Cached {pareto_shapes[3]}','Non-cached']
    bar_values = [cached_time, cached_time1, cached_time2, cached_time3, non_cached_time]
    hit_rates = [hit_rate, hit_rate1, hit_rate2, hit_rate3, None]

    fig, ax = plt.subplots()
    print(bar_values)
    bars = ax.bar(bar_labels, bar_values)
    

    ax.set_ylabel('Time (s)')
    ax.set_title(f'{num_queries} queries with {max_cache_keys} cache keys {cache_policy} cache policy')

    # Add hit rate labels above each bar
    for i, bar in enumerate(bars):
        if hit_rates[i] is not None:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{hit_rates[i]:.2%}", ha='center', va='bottom')

    plt.savefig(f'{num_queries}_{cache_policy}_{max_cache_keys}.png')
    #plt.show()




