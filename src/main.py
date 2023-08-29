import asyncio
import datetime
import os
from urllib.parse import urlparse

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from neo4j import AsyncGraphDatabase, AsyncManagedTransaction, AsyncDriver, Record
from redis.asyncio import Redis

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


async def add_to_weighted_queue(neo4j_driver: AsyncDriver, url: str, producer: AIOKafkaProducer):
    print(url)
    async with neo4j_driver.session(database='pages') as session:
        record = await session.execute_read(get_rank, url)
    if record is not None:
        WEIGHTED_QUEUE.insert(url, record[0])
    else:
        await producer.send(INPUT_TOPIC, url.encode('utf-8'), timestamp_ms=now())


async def send_to_stream(redis: Redis):
    pipe = redis.pipeline()
    urls = [WEIGHTED_QUEUE.pop() for _ in range(BATCH_SIZE)]
    print(f'urls: {urls}')
    stream_names = [OUTPUT_STREAM_NAME.format(urlparse(url).netloc) for url in urls]
    await pipe.zadd(DOMAIN_HEAP_QUEUE, {stream_name: 0 for stream_name in stream_names}, nx=True)
    tasks = [pipe.xadd(stream_name, {'url': url}) for stream_name, url in zip(stream_names, urls)]
    await asyncio.gather(*tasks)
    await pipe.execute()


async def main():
    kafka_uri = os.getenv('KAFKA_URI', 'localhost:9092')
    consumer = AIOKafkaConsumer(INPUT_TOPIC, bootstrap_servers=kafka_uri)
    producer = AIOKafkaProducer(bootstrap_servers=kafka_uri)
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_credentials = os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password')
    neo4j_driver = AsyncGraphDatabase.driver(neo4j_uri, auth=neo4j_credentials)
    redis = Redis(host=os.getenv("REDIS_HOST", "localhost"), port=os.getenv("REDIS_PORT", 6379))
    print('Starting...')
    try:
        await asyncio.gather(*(consumer.start(), producer.start()))
        async for msg in consumer:
            await add_to_weighted_queue(neo4j_driver, msg.value.decode('utf-8'), producer)
            if len(WEIGHTED_QUEUE.elements) >= BATCH_SIZE:
                print('Sending to stream')
                await send_to_stream(redis)
    finally:
        await asyncio.gather(*(consumer.stop(), producer.stop()))


if __name__ == '__main__':
    asyncio.run(main())
