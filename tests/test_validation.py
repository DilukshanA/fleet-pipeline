from common.validation import validate_trip_event, validate_cost_row

GOOD_EVENT = {
    "event_id": "e1", "trip_id": "t1", "driver_id": "d1", "vehicle_id": "v1",
    "lat": 6.90, "lon": 79.88, "speed": 30.0, "status": "on_trip",
    "fare": 450.0, "timestamp": "2026-01-01T10:00:00Z",
}


def test_valid_event_passes():
    ok, reason = validate_trip_event(GOOD_EVENT)
    assert ok is True
    assert reason is None


def test_missing_field_fails():
    bad = dict(GOOD_EVENT)
    del bad["lat"]
    ok, reason = validate_trip_event(bad)
    assert ok is False
    assert reason == "missing_field:lat"


def test_negative_fare_fails():
    bad = dict(GOOD_EVENT, fare=-10)
    ok, reason = validate_trip_event(bad)
    assert ok is False
    assert reason == "negative_fare"


def test_unknown_status_fails():
    bad = dict(GOOD_EVENT, status="parked")
    ok, reason = validate_trip_event(bad)
    assert ok is False
    assert reason == "unknown_status"


def test_out_of_range_coordinates_fail():
    bad = dict(GOOD_EVENT, lat=200.0)
    ok, reason = validate_trip_event(bad)
    assert ok is False
    assert reason == "coordinates_out_of_range"


GOOD_COST = {
    "vehicle_id": "v1", "fuel_cost": 100.0, "maintenance_cost": 20.0,
    "distance_covered": 80.0, "service_flag": False,
}


def test_valid_cost_row_passes():
    ok, reason = validate_cost_row(GOOD_COST)
    assert ok is True


def test_negative_cost_fails():
    bad = dict(GOOD_COST, fuel_cost=-5)
    ok, reason = validate_cost_row(bad)
    assert ok is False
    assert reason == "negative_value"
