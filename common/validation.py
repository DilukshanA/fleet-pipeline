"""Validation rules shared by the streaming job (for trip events) and the
Airflow batch job (for cost rows). Kept dependency-free (no pandas/pyspark
imports) so it can be unit tested in isolation and imported from a Spark UDF.

NaN guard: Python's float() happily parses the strings "nan"/"NaN"/"inf" into
real IEEE-754 NaN/Infinity without raising, and pandas' read_csv silently
turns common "missing value" tokens (including our own "N/A" corruption
marker) into NaN on read. Either path lets a NaN slip past a plain
`except (TypeError, ValueError)` guard -- NaN comparisons like `x < 0` are
always False, so a NaN cost or fare would otherwise sail through as "valid"
and later break JSON serialisation (Postgres NUMERIC can store NaN; jsonable
encoders generally cannot). _is_bad_float() closes that gap explicitly.
"""
import math

from common.zones import LAT_MIN, LAT_MAX, LON_MIN, LON_MAX


def _is_bad_float(value: float) -> bool:
    return math.isnan(value) or math.isinf(value)

VALID_STATUSES = {"idle", "enroute", "on_trip"}

REQUIRED_TRIP_FIELDS = [
    "event_id", "trip_id", "driver_id", "vehicle_id",
    "lat", "lon", "speed", "status", "fare", "timestamp",
]


def validate_trip_event(event: dict):
    """Returns (is_valid: bool, reason: str|None)."""
    for field in REQUIRED_TRIP_FIELDS:
        if field not in event or event[field] is None:
            return False, f"missing_field:{field}"

    try:
        lat = float(event["lat"])
        lon = float(event["lon"])
    except (TypeError, ValueError):
        return False, "non_numeric_coordinates"
    if _is_bad_float(lat) or _is_bad_float(lon):
        return False, "non_numeric_coordinates"

    if not (LAT_MIN - 0.5 <= lat <= LAT_MAX + 0.5) or not (LON_MIN - 0.5 <= lon <= LON_MAX + 0.5):
        return False, "coordinates_out_of_range"

    try:
        fare = float(event["fare"])
    except (TypeError, ValueError):
        return False, "non_numeric_fare"
    if _is_bad_float(fare) or fare < 0:
        return False, "negative_fare"

    try:
        speed = float(event["speed"])
    except (TypeError, ValueError):
        return False, "non_numeric_speed"
    if _is_bad_float(speed) or speed < 0 or speed > 200:
        return False, "speed_out_of_range"

    if event["status"] not in VALID_STATUSES:
        return False, "unknown_status"

    return True, None


REQUIRED_COST_FIELDS = [
    "vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag",
]


def validate_cost_row(row: dict):
    for field in REQUIRED_COST_FIELDS:
        if field not in row or row[field] is None or row[field] == "":
            return False, f"missing_field:{field}"
    try:
        fuel_cost = float(row["fuel_cost"])
        maintenance_cost = float(row["maintenance_cost"])
        distance = float(row["distance_covered"])
    except (TypeError, ValueError):
        return False, "non_numeric_cost_or_distance"

    if any(_is_bad_float(v) for v in (fuel_cost, maintenance_cost, distance)):
        return False, "non_numeric_cost_or_distance"

    if fuel_cost < 0 or maintenance_cost < 0 or distance < 0:
        return False, "negative_value"

    return True, None
