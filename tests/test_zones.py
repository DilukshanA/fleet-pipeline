from common.zones import zone_for, ALL_ZONES, LAT_MIN, LAT_MAX, LON_MIN, LON_MAX


def test_all_named_zones_are_unique():
    assert len(ALL_ZONES) == 9
    assert len(set(ALL_ZONES)) == 9


def test_center_of_box_maps_to_city_center():
    lat = (LAT_MIN + LAT_MAX) / 2
    lon = (LON_MIN + LON_MAX) / 2
    assert zone_for(lat, lon) == "City-Center"


def test_north_west_corner():
    assert zone_for(LAT_MAX - 0.001, LON_MIN + 0.001) == "North-West"


def test_south_east_corner():
    assert zone_for(LAT_MIN + 0.001, LON_MAX - 0.001) == "South-East"


def test_out_of_range_is_clamped_not_raised():
    # Should not throw, even for wildly out-of-range coordinates.
    zone_for(0.0, 0.0)
    zone_for(90.0, 180.0)
