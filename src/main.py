import asyncio
import datetime
import traceback
from urllib.parse import urlparse

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from neo4j import AsyncGraphDatabase, AsyncManagedTransaction, AsyncDriver, Record
from common_utils import persistent_execution
from common_utils.logger import Logger

from settings import Settings
from weighted_queue import WeightedQueue

INPUT_TOPIC = 'urls_unprioritized'
OUTPUT_STREAM_NAME = 'domain_queue:{}'
DOMAIN_HEAP_QUEUE = 'domain_heap_queue'
BATCH_SIZE = 256
WEIGHTED_QUEUE = WeightedQueue()


def now():
    return datetime.datetime.now().timestamp() * 1000


async def get_rank(tx: AsyncManagedTransaction, url: str) -> Record | None:
    result = await tx.run('MATCH (page:Page {url: $url}) RETURN page.rank AS rank', url=url)
    return await result.single()


async def add_to_weighted_queue(neo4j_driver: AsyncDriver, url: str, producer: AIOKafkaProducer, logger: Logger):
    logger.debug(f'Adding {url} to weighted queue')
    async with neo4j_driver.session() as session:
        record = await session.execute_read(get_rank, url)
    if record is not None:
        logger.debug(f'Got rank {record[0]} for {url}')
        WEIGHTED_QUEUE.insert(url, record[0])
    else:
        logger.debug(f'No rank found for {url}, resending to kafka')
        await producer.send(INPUT_TOPIC, url.encode('utf-8'), timestamp_ms=now())


async def send_to_stream(redis: aioredis.Redis):
    pipe = redis.pipeline()
    urls = [WEIGHTED_QUEUE.pop() for _ in range(BATCH_SIZE)]
    stream_names = [OUTPUT_STREAM_NAME.format(urlparse(url).netloc) for url in urls]
    await pipe.zadd(DOMAIN_HEAP_QUEUE, {stream_name: 0 for stream_name in stream_names}, nx=True)
    tasks = [pipe.xadd(stream_name, {'url': url}) for stream_name, url in zip(stream_names, urls)]
    await asyncio.gather(*tasks)
    await pipe.execute()


async def main():
    settings = Settings()
    logger = Logger('prioritizer', settings.log_level)
    consumer = AIOKafkaConsumer(INPUT_TOPIC, bootstrap_servers=settings.kafka_uri)
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_uri)
    neo4j_driver = AsyncGraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
    redis = await aioredis.from_url(settings.redis_uri)
    logger.info('Starting...')
    try:
        logger.info('Starting consumer/producer')
        start_kwargs = {'tries': 5, 'delay': 5, 'backoff': 5, 'logger_': logger}
        await persistent_execution(consumer.start, **start_kwargs)
        await persistent_execution(producer.start, **start_kwargs)
        logger.info('Started consumer/producer')
        async for msg in consumer:
            decoded = msg.value.decode('utf-8')
            logger.debug(f"Received {decoded} from {msg.topic}:{msg.partition}:{msg.offset}")
            await add_to_weighted_queue(neo4j_driver, decoded, producer, logger)
            if len(WEIGHTED_QUEUE.elements) >= BATCH_SIZE:
                logger.debug(f"Sending {len(WEIGHTED_QUEUE.elements)} to stream")
                await send_to_stream(redis)
    except Exception:
        traceback.print_exc()
    finally:
        await asyncio.gather(*(consumer.stop(), producer.stop()))


if __name__ == '__main__':
    asyncio.run(main())
