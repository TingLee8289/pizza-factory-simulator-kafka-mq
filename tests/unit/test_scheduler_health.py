from pizza.scheduler.health import stale_stations


def test_stale_detection():
    assert stale_stations({"spin": 0.0, "bake": 9.0}, 10.0, 1.0, 3) == {"spin"}


def test_boundary():
    assert stale_stations({"a": 0.0}, 3.0, 1.0, 3) == set()
    assert stale_stations({"a": 0.0}, 3.001, 1.0, 3) == {"a"}


def test_disabled_and_absent():
    assert stale_stations({"a": 0.0}, 100.0, 0, 3) == set()
    assert stale_stations({}, 100.0, 1.0, 3) == set()
