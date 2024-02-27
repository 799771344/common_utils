import asyncio

from common_utils.async_redis_utils import AsyncRedis
from common_utils.redis_utils import Redis


async def main():
    redis_helper = AsyncRedis(host='127.0.0.1', port=1233, db=0)

    await redis_helper.connect()  # Connect to Redis server

    await redis_helper.set_key('test_key', 'Hello, Async Redis!')  # Set a key
    value = await redis_helper.get_key('test_key')  # Get the key
    print(value)  # Print the result

    await redis_helper.close()  # Close connection


if __name__ == '__main__':
    # 同步
    # 无需密码
    r = Redis(host='127.0.0.1', port=1233, db=0)
    # 如果Redis服务设置了密码，可以通过password参数来指定
    # r = Redis(host='127.0.0.1', port=1233, db=0, password='123')

    r.set('name', 'tom')

    print(r.get('name'))

    # 异步
    asyncio.run(main())
