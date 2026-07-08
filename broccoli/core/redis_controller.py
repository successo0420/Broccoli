import redis


class RedisController:
    """
    A controller for managing Redis connections and operations. Use the `get_client` method to obtain a Redis client instance.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self._redis = redis.from_url(redis_url, decode_responses=True)

    def delete_all_keys(self):
        """Delete all keys in Redis (use with caution)."""
        return self._redis.flushdb()

    def get_client(self):
        """Get the Redis client instance."""
        return self._redis

    def view_all_keys(self):
        """View all keys in Redis."""
        return self._redis.keys("*")

    def view_key(self, key: str):
        """View value of a specific key."""
        return self._redis.get(key)

    def delete_key(self, key: str):
        """Delete a specific key."""
        return self._redis.delete(key)

    def get_count(self, pattern="*"):
        """Get count of keys matching a pattern."""
        return len(self._redis.keys(pattern))

    def view_all(self, pattern="*"):
        """View all keys and their values matching a pattern."""
        keys = self._redis.keys(pattern)
        return {key.decode(): self._redis.get(key).decode() for key in keys}
