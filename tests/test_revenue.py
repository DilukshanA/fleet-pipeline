from common.revenue import compute_profit, compute_profit_per_km, compute_margin, is_unprofitable


def test_compute_profit_basic():
    assert compute_profit(1000, 200, 100) == 700


def test_compute_profit_can_go_negative():
    profit = compute_profit(100, 150, 60)
    assert profit == -110
    assert is_unprofitable(profit) is True


def test_profit_per_km_zero_distance_is_none():
    assert compute_profit_per_km(700, 0) is None


def test_profit_per_km_normal():
    assert compute_profit_per_km(100, 50) == 2.0


def test_margin_zero_earnings_is_none():
    assert compute_margin(-50, 0) is None


def test_margin_normal():
    assert compute_margin(250, 1000) == 0.25
