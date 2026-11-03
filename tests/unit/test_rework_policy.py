from pizza.scheduler.rework import DEFAULT_REWORK_LIMITS, DeadLetter, Rework, decide


def test_spin_reworks_until_limit():
    limits = DEFAULT_REWORK_LIMITS
    assert decide("spin", 1, limits) == Rework(next_visit=2)
    assert decide("spin", 2, limits) == Rework(3)
    assert decide("spin", 3, limits) == DeadLetter(reason="rework_exhausted")


def test_zero_limit_and_unknown_dead_letter():
    limits = DEFAULT_REWORK_LIMITS
    assert isinstance(decide("bake", 1, limits), DeadLetter)
    assert isinstance(decide("nope", 1, limits), DeadLetter)


def test_custom_table():
    assert decide("sauce", 1, {"sauce": 1}) == Rework(2)
    assert isinstance(decide("sauce", 2, {"sauce": 1}), DeadLetter)
