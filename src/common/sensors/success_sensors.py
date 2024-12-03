from datetime import datetime, timedelta, tzinfo
from zoneinfo import ZoneInfo

from airflow.sensors.filesystem import FileSensor

from src.common.datasets import Dataset

def wait_for_dataset(dataset: Dataset, start_date: datetime, end_date: datetime):
    paths = dataset.get_paths(start_date, end_date)
    return [FileSensor(filepath=pth) for pth in paths]

def wait_for_latest_dataset(dataset: Dataset, intervals = 0):
    now = datetime.now(tz=ZoneInfo("UTC"))
    paths = dataset.get_latest_paths(now, intervals)
    return [FileSensor(filepath=pth) for pth in paths]