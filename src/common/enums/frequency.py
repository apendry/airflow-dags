from enum import Enum

from dateutil.rrule import MONTHLY, WEEKLY, DAILY, HOURLY

# Convenience wrapper to access rrule's frequency
class Frequency(Enum):
    HOURLY = HOURLY,
    DAILY = DAILY,
    WEEKLY = WEEKLY
    MONTHLY = MONTHLY

