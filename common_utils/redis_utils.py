import redis


class Redis:
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.client = redis.StrictRedis(host=host, port=port, db=db, password=password, decode_responses=True)

    def set(self, key, value):
        return self.client.set(key, value)

    def get(self, key):
        return self.client.get(key)

    def delete(self, key):
        return self.client.delete(key)

    def publish(self, channel, message):
        return self.client.publish(channel, message)

    def subscribe_to_channel(self, channel):
        pubsub = self.client.pubsub()
        pubsub.subscribe(channel)
        return pubsub

    def hset(self, name, key, value):
        return self.client.hset(name, key, value)

    def hmset(self, name, value):
        return self.client.hmset(name, value)

    def hgetall(self, name):
        return self.client.hgetall(name)

    def hget(self, name, key):
        return self.client.hget(name, key)

    def zadd(self, name, score, value):
        return self.client.zadd(name, score, value)

    def zrange(self, name, start, end):
        return self.client.zrange(name, start, end)

    def zrevrange(self, name, start, end):
        return self.client.zrevrange(name, start, end)

    def zrangebyscore(self, name, min, max):
        return self.client.zrangebyscore(name, min, max)
