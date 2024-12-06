import pendulum
from flask_session.sessions import total_seconds

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
        self.timeout = self.__generate_timeout__()


    # Given the frequency append the base_dir with a jinja datetime template.
    def __generate_path_template__(self) -> str:
        suffix = self.suffix_dir + "/" if self.suffix_dir else ""
        match self.frequency:
            case Frequency.HOURLY:
                return f"{self.base_dir}/%Y/%m/%d/%H/{suffix}"
            case Frequency.DAILY:
                return f"{self.base_dir}/%Y/%m/%d/{suffix}"
            case Frequency.WEEKLY:
                return f"{self.base_dir}/%Y/%m/%d/{suffix}"
            case Frequency.MONTHLY:
                return f"{self.base_dir}/%Y/%m/{suffix}"

    def __generate_timeout__(self) -> float:
        match self.frequency:
            case Frequency.HOURLY:
                return pendulum.duration(hours=1).total_seconds()
            case Frequency.DAILY:
                return pendulum.duration(days=1).total_seconds()
            case Frequency.WEEKLY:
                return pendulum.duration(weeks=1).total_seconds()
            case Frequency.MONTHLY:
                return pendulum.duration(months=1).total_seconds()