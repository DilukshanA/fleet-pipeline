"""Streaming source: simulates GPS/telemetry events for a fleet of vehicles
and publishes them to the Kafka topic KAFKA_TOPIC_TRIPS, keyed by vehicle_id
(so all of one vehicle's events land in the same partition, in order -- the
idle-detection logic in the streaming job depends on that).

Runs on the simulated clock defined in common/sim_clock.py. Demand (how
often idle vehicles pick up a trip) varies by simulated hour, so "earnings
by time of day" has a real pattern to show in the demo.

Fare is recognised once, on the event where a trip starts (idle -> enroute),
not repeated on every telemetry ping. That is a deliberate simplification so
that summing `fare` over a window gives correct earnings without having to
de-duplicate a repeated value across many pings of the same trip.
"""
import json
import random
import time
import uuid
from datetime import timedelta

from kafka import KafkaProducer

from common import config
from common.logging_utils import get_logger
from common.sim_clock import sim_now
from common.zones import LAT_MIN, LAT_MAX, LON_MIN, LON_MAX

log = get_logger("ingestion.trip_simulator")

STATUSES = ("idle", "enroute", "on_trip")


class Vehicle:
    __slots__ = (
        "vehicle_id", "driver_id", "status", "lat", "lon",
        "trip_id", "trip_ticks_left", "idle_trip_id",
    )

    def __init__(self, vehicle_id, driver_id, lat, lon):
        self.vehicle_id = vehicle_id
        self.driver_id = driver_id
        self.status = "idle"
        self.lat = lat
        self.lon = lon
        self.trip_id = None
        self.trip_ticks_left = 0
        self.idle_trip_id = f"idle-{vehicle_id}-{uuid.uuid4().hex[:6]}"


def demand_factor(sim_hour: float) -> float:
    """Rough day shape: morning + evening peaks, quiet overnight."""
    if 7 <= sim_hour < 9 or 17 <= sim_hour < 19:
        return 0.90
    if 9 <= sim_hour < 17:
        return 0.40
    if 0 <= sim_hour < 5:
        return 0.08
    return 0.25


def make_producer() -> KafkaProducer:
    last_err = None
    for attempt in range(30):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
                api_version=(3, 7, 0),
                retries=5,
                linger_ms=50,
            )
            log.info("connected_to_kafka", bootstrap=config.KAFKA_BOOTSTRAP)
            return producer
        except Exception as exc:  # noqa: BLE001 - retry loop, log and back off
            last_err = exc
            log.warning("kafka_connect_retry", attempt=attempt, error=str(exc))
            time.sleep(2)
    raise RuntimeError(f"could not connect to kafka: {last_err}")


def init_vehicles(rng: random.Random):
    vehicles = {}
    for i in range(config.SIM_VEHICLE_COUNT):
        vehicle_id = f"veh-{i:03d}"
        driver_id = f"drv-{i:03d}"
        lat = rng.uniform(LAT_MIN, LAT_MAX)
        lon = rng.uniform(LON_MIN, LON_MAX)
        vehicles[vehicle_id] = Vehicle(vehicle_id, driver_id, lat, lon)
    return vehicles


def jitter_position(v: Vehicle, rng: random.Random):
    step_lat = rng.uniform(-0.004, 0.004)
    step_lon = rng.uniform(-0.004, 0.004)
    v.lat = min(max(v.lat + step_lat, LAT_MIN), LAT_MAX)
    v.lon = min(max(v.lon + step_lon, LON_MIN), LON_MAX)


def step_vehicle(v: Vehicle, sim_time, factor: float, rng: random.Random) -> dict:
    """Advance one vehicle by one tick and return the telemetry event to emit."""
    fare = 0.0

    if v.status == "idle":
        jitter_position(v, rng)
        speed = 0.0
        if rng.random() < 0.15 * factor:
            v.status = "enroute"
            v.trip_id = f"trip-{v.vehicle_id}-{uuid.uuid4().hex[:8]}"
            v.trip_ticks_left = rng.randint(2, 4)
            distance_estimate = rng.uniform(2, 18)
            fare = round(60 + distance_estimate * rng.uniform(35, 65), 2)  # upfront fare
            trip_id = v.trip_id
        else:
            trip_id = v.idle_trip_id

    elif v.status == "enroute":
        jitter_position(v, rng)
        speed = rng.uniform(15, 40)
        v.trip_ticks_left -= 1
        trip_id = v.trip_id
        if v.trip_ticks_left <= 0:
            v.status = "on_trip"
            v.trip_ticks_left = rng.randint(3, 12)

    else:  # on_trip
        jitter_position(v, rng)
        speed = rng.choice([0.0, 0.0] + [rng.uniform(10, 60) for _ in range(6)])
        v.trip_ticks_left -= 1
        trip_id = v.trip_id
        if v.trip_ticks_left <= 0:
            v.status = "idle"
            v.idle_trip_id = f"idle-{v.vehicle_id}-{uuid.uuid4().hex[:6]}"
            v.trip_id = None

    event = {
        "event_id": str(uuid.uuid4()),
        "trip_id": trip_id,
        "driver_id": v.driver_id,
        "vehicle_id": v.vehicle_id,
        "lat": round(v.lat, 6),
        "lon": round(v.lon, 6),
        "speed": round(speed, 1),
        "status": v.status if trip_id != getattr(v, "idle_trip_id", None) or v.status != "idle" else "idle",
        "fare": fare,
        "timestamp": sim_time.isoformat(),
    }
    return event


def inject_messiness(event: dict, rng: random.Random):
    """With probability SIM_MESSY_RATE, corrupt or duplicate the event so the
    pipeline has real bad data to clean, drop or quarantine (and count)."""
    if rng.random() >= config.SIM_MESSY_RATE:
        return [event]

    kind = rng.choice([
        "duplicate", "null_coords", "out_of_range_coords",
        "negative_fare", "unknown_status", "late_timestamp", "missing_field",
    ])
    bad = dict(event)

    if kind == "duplicate":
        return [event, dict(event)]  # same event_id twice -> tests dedup
    if kind == "null_coords":
        bad["lat"] = None
    elif kind == "out_of_range_coords":
        bad["lat"] = 999.0
    elif kind == "negative_fare":
        bad["fare"] = -abs(bad["fare"]) - 25
    elif kind == "unknown_status":
        bad["status"] = "parked"
    elif kind == "late_timestamp":
        ts = event["timestamp"]
        # shove the timestamp a few simulated minutes into the past
        from datetime import datetime
        dt = datetime.fromisoformat(ts) - timedelta(minutes=rng.randint(5, 30))
        bad["timestamp"] = dt.isoformat()
    elif kind == "missing_field":
        bad.pop("speed", None)

    return [bad]


def run():
    rng = random.Random()
    log.info(
        "trip_simulator_starting",
        vehicles=config.SIM_VEHICLE_COUNT,
        emit_interval_s=config.SIM_EMIT_INTERVAL_SECONDS,
        sim_day_real_seconds=config.SIM_DAY_REAL_SECONDS,
        topic=config.KAFKA_TOPIC_TRIPS,
    )
    producer = make_producer()
    vehicles = init_vehicles(rng)
    tick = 0

    while True:
        tick_start = time.time()
        now_sim = sim_now()
        sim_hour = now_sim.hour + now_sim.minute / 60
        factor = demand_factor(sim_hour)

        produced = 0
        for v in vehicles.values():
            event = step_vehicle(v, now_sim, factor, rng)
            for out_event in inject_messiness(event, rng):
                producer.send(
                    config.KAFKA_TOPIC_TRIPS,
                    key=out_event.get("vehicle_id") or "unknown",
                    value=out_event,
                )
                produced += 1
        producer.flush()

        log.info(
            "tick_emitted",
            tick=tick,
            events=produced,
            sim_time=now_sim.isoformat(),
            demand_factor=round(factor, 2),
        )
        tick += 1
        elapsed = time.time() - tick_start
        time.sleep(max(0.0, config.SIM_EMIT_INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    run()
