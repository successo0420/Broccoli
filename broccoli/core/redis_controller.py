import redis


class RedisController:
    """
    A controller for managing Redis connections and operations. Use the `get_client` method to obtain a Redis client instance.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        decode_responses: bool = True,
        socket_timeout: float = None,
        socket_connect_timeout: float = None,
        health_check_interval: int = 30,
        retry_on_timeout: bool = True,
        max_connections: int = None,
    ):
        redis_kwargs = {
            "decode_responses": decode_responses,
            "health_check_interval": health_check_interval,
            "retry_on_timeout": retry_on_timeout,
        }
        if socket_timeout is not None:
            redis_kwargs["socket_timeout"] = socket_timeout
        if socket_connect_timeout is not None:
            redis_kwargs["socket_connect_timeout"] = socket_connect_timeout
        if max_connections is not None:
            redis_kwargs["max_connections"] = max_connections
        self._redis = redis.from_url(redis_url, **redis_kwargs)

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
        result = {}
        for key in keys:
            output_key = key.decode() if isinstance(key, bytes) else key
            value = self._redis.get(key)
            if isinstance(value, bytes):
                value = value.decode()
            result[output_key] = value
        return result
