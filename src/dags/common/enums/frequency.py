from enum import StrEnum

# Wrapper class for pendulum's units
class Frequency(StrEnum):
    HOURLY = "hours",
    DAILY = "days",
    WEEKLY = "weeks"
    MONTHLY = "months"

