from ..datasets.dataset import Dataset
from ..enums.frequency import Frequency

example_calories_output = Dataset("calories_output", "example", Frequency.HOURLY, "calories_output")
example_calories_log = Dataset("calories_log", "example", Frequency.HOURLY, "calories_log")
example_meta_people = Dataset("people_meta", "meta", Frequency.DAILY, "people")
example_meta_activity = Dataset("activity_meta", "meta", Frequency.DAILY, "activity")