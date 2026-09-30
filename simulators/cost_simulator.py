"""Daily-batch source: once per simulated day, drops one
costs_YYYY-MM-DD.csv file into the landing folder Airflow watches.

Written atomically (temp name, then os.replace) so Airflow's file sensor
never reads a half-written file. Occasionally omits a vehicle entirely
(missing data) or writes a corrupt row (negative cost, blank field) so the
batch layer has real bad data to reject and log.
"""
import csv
import os
import random
import time
from datetime import timedelta

from common import config
from common.logging_utils import get_logger
from common.sim_clock import sim_day_index, _BASE_DATE  # noqa: SLF001 - reuse base date

log = get_logger("ingestion.cost_simulator")

FIELDNAMES = ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag", "cost_date"]


def _corrupt(row: dict, rng: random.Random) -> dict:
    kind = rng.choice(["negative_cost", "blank_distance", "text_in_number"])
    bad = dict(row)
    if kind == "negative_cost":
        bad["fuel_cost"] = -abs(float(bad["fuel_cost"]))
    elif kind == "blank_distance":
        bad["distance_covered"] = ""
    elif kind == "text_in_number":
        bad["maintenance_cost"] = "N/A"
    return bad


def build_rows(day_index: int, rng: random.Random):
    cost_date = (_BASE_DATE + timedelta(days=day_index)).strftime("%Y-%m-%d")
    rows = []
    for i in range(config.SIM_VEHICLE_COUNT):
        if rng.random() < 0.03:
            continue  # vehicle missing from today's cost file entirely

        vehicle_id = f"veh-{i:03d}"
        distance = round(rng.uniform(40, 230), 1)
        fuel_cost = round(distance * rng.uniform(8, 14), 2)
        maintenance_cost = round(rng.uniform(0, 3200), 2) if rng.random() < 0.2 else 0.0
        service_flag = rng.random() < 0.1

        row = {
            "vehicle_id": vehicle_id,
            "fuel_cost": fuel_cost,
            "maintenance_cost": maintenance_cost,
            "distance_covered": distance,
            "service_flag": service_flag,
            "cost_date": cost_date,
        }
        if rng.random() < config.SIM_MESSY_RATE:
            row = _corrupt(row, rng)
        rows.append(row)
    return cost_date, rows


def write_cost_file(day_index: int, rng: random.Random):
    os.makedirs(config.LANDING_DIR, exist_ok=True)
    cost_date, rows = build_rows(day_index, rng)

    final_path = os.path.join(config.LANDING_DIR, f"costs_{cost_date}.csv")
    tmp_path = os.path.join(config.LANDING_DIR, f".costs_{cost_date}.csv.tmp")

    with open(tmp_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, final_path)  # atomic on POSIX and on Windows (same filesystem)

    log.info("cost_file_written", path=final_path, rows=len(rows), cost_date=cost_date)


def run():
    log.info(
        "cost_simulator_starting",
        sim_day_real_seconds=config.SIM_DAY_REAL_SECONDS,
        landing_dir=config.LANDING_DIR,
    )
    rng = random.Random()
    last_written_index = -1

    while True:
        idx = sim_day_index()
        if idx > 0 and idx - 1 > last_written_index:
            completed_day_index = idx - 1
            write_cost_file(completed_day_index, rng)
            last_written_index = completed_day_index
        time.sleep(1)


if __name__ == "__main__":
    run()
