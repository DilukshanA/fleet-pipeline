"""Zone lookup shared by the streaming job and any batch reporting that
needs to bucket a lat/lon into a named service area.

We simulate a fictional city as a 3x3 grid inside a fixed bounding box.
Both the simulator (to spread vehicles realistically) and the Spark job
(to enrich trip events with a zone) import BOUNDS from here so they agree
on the same box.
"""

# Fictional city bounding box (approx 12km x 12km).
LAT_MIN, LAT_MAX = 6.85, 6.97
LON_MIN, LON_MAX = 79.83, 79.95

_GRID_SIZE = 3
_ZONE_NAMES = [
    "North-West", "North-Central", "North-East",
    "Mid-West", "City-Center", "Mid-East",
    "South-West", "South-Central", "South-East",
]


def zone_for(lat: float, lon: float) -> str:
    """Map a lat/lon to one of 9 named zones. Out-of-range coordinates are
    clamped to the nearest edge cell rather than rejected here -- the
    validation stage decides whether an out-of-range point is bad data.
    """
    lat_frac = (lat - LAT_MIN) / (LAT_MAX - LAT_MIN)
    lon_frac = (lon - LON_MIN) / (LON_MAX - LON_MIN)
    lat_frac = min(max(lat_frac, 0.0), 0.999999)
    lon_frac = min(max(lon_frac, 0.0), 0.999999)

    row = int(lat_frac * _GRID_SIZE)  # 0 = south .. 2 = north
    col = int(lon_frac * _GRID_SIZE)  # 0 = west .. 2 = east

    # _ZONE_NAMES is laid out north-to-south, so flip the row.
    grid_row = (_GRID_SIZE - 1) - row
    idx = grid_row * _GRID_SIZE + col
    return _ZONE_NAMES[idx]


def random_point_in_zone(rng, zone_name: str):
    """Used by the trip simulator to place a vehicle inside a named zone."""
    idx = _ZONE_NAMES.index(zone_name)
    grid_row, col = divmod(idx, _GRID_SIZE)
    row = (_GRID_SIZE - 1) - grid_row

    lat_step = (LAT_MAX - LAT_MIN) / _GRID_SIZE
    lon_step = (LON_MAX - LON_MIN) / _GRID_SIZE

    lat = LAT_MIN + row * lat_step + rng.random() * lat_step
    lon = LON_MIN + col * lon_step + rng.random() * lon_step
    return lat, lon


ALL_ZONES = list(_ZONE_NAMES)
