# Django Redis Cache for API System

This project is a Django application that uses a custom Redis caching system to store and manage API responses. The caching system is designed to work with Open Library API calls, but it can be easily adapted to other APIs.

The custom Redis caching system distributes cached data across multiple Redis instances and provides various cache eviction strategies. It also includes functionality for performance testing and memory estimation.

## Getting Started

To get started with the project, follow the steps below.

### Prerequisites

- Docker
- Docker Compose

### Running the application

1. Build the Docker containers:

   docker-compose build

2. Start the Docker containers:

   docker-compose up

### Testing the cache

To test the cache, run the following command:

    docker-compose exec web python manage.py test_code

### Resetting the cache

To reset the cache, run the following command:

    docker-compose exec web python manage.py reset_cache

### Checking Redis cache memory info

To check the memory information of the Redis cache, run the following command:

    redis-cli info memory

## Features

- Custom Redis caching system for API responses
- Distribution of cache data across multiple Redis instances
- Various cache eviction strategies (e.g., LRU)
- Performance testing and memory estimation

## License

This project is open-source and available under the MIT License. See the `LICENSE` file for more details.
