import asyncio
import aioredis


class AsyncRedis:
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.client = aioredis.StrictRedis(host=host, port=port, db=db, password=password, decode_responses=True)
        self.redis = None

    async def connect(self):

        self.redis = await aioredis.connection(self.redis_url)

    async def close(self):
        if self.redis:
            self.redis.close()
            await self.redis.wait_closed()

    async def set_key(self, key, value):
        await self.redis.set(key, value)

    async def get_key(self, key):
        value = await self.redis.get(key)
        if value is not None:
            return value.decode('utf-8')
        return None


# Usage example with asyncio loop:

async def main():
    redis_helper = AsyncRedis('redis://localhost')

    await redis_helper.connect()  # Connect to Redis server

    await redis_helper.set_key('test_key', 'Hello, Async Redis!')  # Set a key
    value = await redis_helper.get_key('test_key')  # Get the key
    print(value)  # Print the result

    await redis_helper.close()  # Close connection


if __name__ == '__main__':
    asyncio.run(main())