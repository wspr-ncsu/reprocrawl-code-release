from threading import Thread, Event

import redis

from .api import BarrierContext, TimeoutError


def test_happy_ab():
    a_has_gone = Event()
    winner_a = None
    winner_b = None

    r = redis.Redis()
    r.delete("test_happy")

    def task_a():
        nonlocal winner_a
        with BarrierContext() as bc:
            winner_a = bc.sync("test_happy", 2, "AAAA", _wait_hook=a_has_gone.set)

    def task_b():
        nonlocal winner_b
        with BarrierContext() as bc:
            a_has_gone.wait()
            winner_b = bc.sync("test_happy", 2, "BBBB")

    a = Thread(target=task_a)
    b = Thread(target=task_b)
    a.start()
    b.start()
    a.join()
    b.join()

    assert winner_a == winner_b == "BBBB"


def test_happy_ba():
    b_has_gone = Event()
    winner_a = None
    winner_b = None

    r = redis.Redis()
    r.delete("test_happy")

    def task_a():
        nonlocal winner_a
        with BarrierContext() as bc:
            b_has_gone.wait()
            winner_a = bc.sync("test_happy", 2, "AAAA",)

    def task_b():
        nonlocal winner_b
        with BarrierContext() as bc:
            winner_b = bc.sync("test_happy", 2, "BBBB", _wait_hook=b_has_gone.set)

    a = Thread(target=task_a)
    b = Thread(target=task_b)
    a.start()
    b.start()
    a.join()
    b.join()

    assert winner_a == winner_b == "AAAA"


def test_timeout():
    import pytest

    r = redis.Redis()
    r.delete("test_timeout")

    with BarrierContext() as bc:
        with pytest.raises(TimeoutError):
            bc.sync("test_timeout", 2, "this will never sync", timeout=1.0)


if __name__ == "__main__":
    test_happy_ab()
