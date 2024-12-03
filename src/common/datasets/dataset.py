from datetime import datetime, timedelta

from dateutil.rrule import rrule
from typing_extensions import overload

from src.common.enums.frequency import Frequency

# Helper class to define dataset locations in the respective data lake.
class Dataset:

    def __init__(self, name: str, base_dir: str, frequency: Frequency, suffix_dir = None):
        self.name = name
        self.base_dir = base_dir
        self.frequency = frequency

        # Optional
        self.suffix_dir = suffix_dir

        # Generated
        self.path_template = self.__generate_path_template__()


    # Given the frequency append the base_dir with a jinja datetime template.
    def __generate_path_template__(self):
        match self.frequency:
            case Frequency.HOURLY:
                return f"{self.base_dir}/%Y/%m/%d/%H/{self.suffix_dir}/"
            case Frequency.DAILY:
                return f"{self.base_dir}/%Y/%m/%d/{self.suffix_dir}/"
            case Frequency.WEEKLY:
                # Here for custom weekly implementation in future
                return f"{self.base_dir}/%Y/%m/%d/{self.suffix_dir}/"
            case Frequency.MONTHLY:
                return f"{self.base_dir}/%Y/%m/{self.suffix_dir}/"

    def get_latest_paths(self, end_date: datetime, intervals = 0):
        assert intervals >= 0
        date_times = rrule(freq=self.frequency, dtstart=end_date, interval=-intervals)
        return [dt.strftime(self.path_template) for dt in date_times]

    def get_paths(self, start_date: datetime, end_date: datetime):
        assert end_date > start_date
        date_times = rrule(freq=self.frequency, dtstart=start_date, until=end_date)
        return [dt.strftime(self.path_template) for dt in date_times]

