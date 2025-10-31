from enum import Enum

class TimeAggregation(Enum):
    HOUR = "3600s"   # 1 hour
    DAY = "86400s"  # 24 hours
    WEEK = "604800s"  # 7 days