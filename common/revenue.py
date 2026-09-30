"""Profit calculation shared by the streaming layer (for sanity-check
comparisons) and the Airflow batch job (for the real daily reconciliation).
Keeping this in one place is how the report answers "how do you keep the
speed layer and batch layer from drifting apart" under a Lambda architecture.
"""


def compute_profit(earnings: float, fuel_cost: float, maintenance_cost: float) -> float:
    return round(earnings - fuel_cost - maintenance_cost, 2)


def compute_profit_per_km(profit: float, distance_covered: float):
    if not distance_covered or distance_covered <= 0:
        return None
    return round(profit / distance_covered, 4)


def compute_margin(profit: float, earnings: float):
    if not earnings or earnings <= 0:
        return None
    return round(profit / earnings, 4)


def is_unprofitable(profit: float) -> bool:
    return profit < 0
