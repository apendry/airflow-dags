from datetime import datetime

from dateutil.rrule import rrule

from ..enums.frequency import Frequency

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
        suffix = self.suffix_dir + "/" if self.suffix_dir else ""
        match self.frequency:
            case Frequency.HOURLY:
                return f"{self.base_dir}/%Y/%m/%d/%H/{suffix}"
            case Frequency.DAILY:
                return f"{self.base_dir}/%Y/%m/%d/{suffix}/"
            case Frequency.WEEKLY:
                # Here for custom weekly implementation in future
                return f"{self.base_dir}/%Y/%m/%d/{suffix}/"
            case Frequency.MONTHLY:
                return f"{self.base_dir}/%Y/%m/{suffix}/"

