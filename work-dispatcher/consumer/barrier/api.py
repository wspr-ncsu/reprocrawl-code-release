import threading
import queue
from types import TracebackType
from typing import Any, Callable, Dict, Optional, Type

import redis


class DynamicPubSub(threading.Thread):
    def __init__(self, pubsub: redis.client.PubSub, quantum: float = 0.5):
        super().__init__()
        self._pubsub = pubsub
        self._quantum = quantum
        self._queue = queue.Queue()
        self._running = threading.Event()
        pubsub.subscribe(
            __dummy__=lambda msg: None
        )  # make sure we have at least one subscription

    def run(self):
        if self._running.is_set():
            return
        self._running.set()
        pubsub = self._pubsub
        quantum = self._quantum
        q = self._queue
        while self._running.is_set():
            pubsub.get_message(ignore_subscribe_messages=True, timeout=quantum)
            while True:
                try:
                    method, args, kwargs, token = q.get_nowait()
                except queue.Empty:
                    break
                else:
                    method(*args, **kwargs)
                    if token:
                        token.set()
        pubsub.close()

    def stop(self):
        self._running.clear()

    def subscribe(self, *args, **kwargs):
        """queue a subscription and wait for its completion before returning"""
        token = threading.Event()
        self._queue.put((self._pubsub.subscribe, args, kwargs, token))
        token.wait()

    def unsubscribe(self, *args, **kwargs):
        """queue an unsubscription (does not wait for completion)"""
        self._queue.put((self._pubsub.unsubscribe, args, kwargs, None))


class TimeoutError(Exception):
    pass


class Release:
    def __init__(self, notifier: "ReleaseNotifier"):
        self._notifier = notifier
        self._event = threading.Event()
        self._data = None

    def trigger(self, msg: dict):
        tag, data = msg["channel"].decode("utf-8"), msg["data"].decode("utf-8")
        self._notifier.unlisten(tag)
        self._data = data
        self._event.set()

    def wait(self, timeout: Optional[float] = None) -> str:
        done = self._event.wait(timeout)
        if done:
            return self._data
        else:
            raise TimeoutError


class ReleaseNotifier:
    def __init__(self, subscriber: DynamicPubSub):
        self._sub = subscriber

    def unlisten(self, tag: str) -> "ReleaseNotifier":
        self._sub.unsubscribe(tag)
        return self

    def listen(self, tag: str) -> Release:
        release = Release(self)
        self._sub.subscribe(**{tag: release.trigger})
        return release


class Barrier:
    def __init__(self, client: redis.Redis, notifier: ReleaseNotifier):
        self._client = client
        self._notifier = notifier

    def sync(
        self,
        tag: str,
        count: int,
        message: str,
        timeout: Optional[float] = None,
        _wait_hook: Optional[callable] = None,
    ) -> str:
        release = self._notifier.listen(tag)

        up_count = self._client.incr(tag)
        if up_count >= count:
            self._client.publish(tag, message)

        if _wait_hook:
            _wait_hook()

        return release.wait(timeout)


def _create_default_pool():
    import os

    REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

    return redis.ConnectionPool.from_url(REDIS_URL)


class BarrierContext:
    def __init__(self, pool: Optional[redis.ConnectionPool] = None):
        if pool:
            self._pool = pool
            self._own_pool = False
        else:
            self._pool = _create_default_pool()
            self._own_pool = True

        self._conn = redis.Redis(connection_pool=pool)
        self._sub: redis.Redis = redis.Redis(connection_pool=pool).pubsub(
            ignore_subscribe_messages=True
        )
        self._sub_pump = DynamicPubSub(self._sub)
        self._sub_pump.start()
        self._notifier = ReleaseNotifier(self._sub_pump)

    def __enter__(self) -> Barrier:
        return Barrier(self._conn, self._notifier)

    def __exit__(
        self,
        exc_type: Optional[Type],
        exc_val: Optional[Exception],
        exc_tb: Optional[TracebackType],
    ):
        self._sub_pump.stop()
        self._sub_pump.join()

        if self._own_pool:
            self._pool.disconnect()
        else:
            for conn in [self._sub, self._conn]:
                conn.close()
