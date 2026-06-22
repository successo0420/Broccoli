import redis


class RedisController:
    def __init__(self, redis_url: str):
        self.__redis = redis.from_url(redis_url)

    def delete_all_keys(self):
        """Delete all keys in Redis (use with caution)."""
        return self.__redis.flushdb()

    def get_client(self):
        """Get the Redis client instance."""
        return self.__redis

    def view_all_keys(self):
        """View all keys in Redis."""
        return self.__redis.keys("*")

    def view_key(self, key: str):
        """View value of a specific key."""
        return self.__redis.get(key)

    def delete_key(self, key: str):
        """Delete a specific key."""
        return self.__redis.delete(key)

    def view_all(self, pattern="*"):
        """View all keys and their values matching a pattern."""
        keys = self.__redis.keys(pattern)
        return {key.decode(): self.__redis.get(key).decode() for key in keys}
