"""Simulated clock shared by every component.

Design (stated in the report too): 1 simulated day = SIM_DAY_REAL_SECONDS
real seconds (default 300s / 5 real minutes). Every component derives the
current simulated time purely from wall-clock time -- no coordination file
or message is needed, so simulators, the streaming job and Airflow all agree
on "what simulated day is it" even though they are separate processes.

How it works:
  - We take the start of the current real UTC calendar day as the anchor
    (real_midnight). Everything is measured relative to that.
  - elapsed_real = seconds since real_midnight.
  - sim_day_index = elapsed_real // SIM_DAY_REAL_SECONDS -> which simulated
    "day cycle" we are currently in (0, 1, 2, ... and it keeps counting up
    across a real day: 288 cycles/day at the default 5-minute setting).
  - Within a cycle, elapsed_real % SIM_DAY_REAL_SECONDS real seconds map to
    a full 0..86399 simulated second-of-day, scaled by the compression
    factor (86400 / SIM_DAY_REAL_SECONDS).
  - sim_datetime = SIM_BASE_DATE + sim_day_index days + sim_second_of_day.

This gives every "simulated day" a fresh midnight-to-midnight cycle every
SIM_DAY_REAL_SECONDS real seconds, which is exactly the cadence the daily
batch cost file and the Airflow DAG need.
"""
from datetime import datetime, timedelta, timezone

from common.config import SIM_BASE_DATE, SIM_DAY_REAL_SECONDS

_SECONDS_PER_SIM_DAY = 86400
_BASE_DATE = datetime.strptime(SIM_BASE_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _real_midnight(now: datetime) -> datetime:
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def compression_factor() -> float:
    return _SECONDS_PER_SIM_DAY / SIM_DAY_REAL_SECONDS


def sim_day_index(now: datetime = None) -> int:
    now = now or datetime.now(timezone.utc)
    elapsed_real = (now - _real_midnight(now)).total_seconds()
    return int(elapsed_real // SIM_DAY_REAL_SECONDS)


def sim_now(now: datetime = None) -> datetime:
    """Current simulated timestamp (timezone-aware, UTC)."""
    now = now or datetime.now(timezone.utc)
    elapsed_real = (now - _real_midnight(now)).total_seconds()
    cycle_elapsed_real = elapsed_real % SIM_DAY_REAL_SECONDS
    sim_second_of_day = cycle_elapsed_real * compression_factor()
    day_idx = int(elapsed_real // SIM_DAY_REAL_SECONDS)
    return _BASE_DATE + timedelta(days=day_idx, seconds=sim_second_of_day)


def sim_date_str(now: datetime = None) -> str:
    return sim_now(now).strftime("%Y-%m-%d")


def seconds_into_current_cycle(now: datetime = None) -> float:
    """Real seconds elapsed since the current simulated day started."""
    now = now or datetime.now(timezone.utc)
    elapsed_real = (now - _real_midnight(now)).total_seconds()
    return elapsed_real % SIM_DAY_REAL_SECONDS


def seconds_left_in_cycle(now: datetime = None) -> float:
    return SIM_DAY_REAL_SECONDS - seconds_into_current_cycle(now)
