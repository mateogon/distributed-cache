from django.core.management.base import BaseCommand
from book_search.views import reset_cache
from book_search import redis_instances

class Command(BaseCommand):
    help = 'Measure cache performance'

    def handle(self, *args, **options):
        import asyncio
        import logging
        logger = logging.getLogger(__name__)
        logger.info("Starting handle()")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(reset_cache())
        logger.info("Finished handle()")
