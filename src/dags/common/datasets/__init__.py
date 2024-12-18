from ..datasets.dataset import Dataset
from ..enums.frequency import Frequency

example_calories_output = Dataset("ExampleCaloriesOutput", "example", Frequency.HOURLY, "calories_output")
example_calories_raw = Dataset("ExampleCaloriesRaw", "example", Frequency.HOURLY, "calories_raw")
example_meta_people = Dataset("ExampleMetaPeople", "meta", Frequency.DAILY, "people")
example_meta_activity = Dataset("ExampleMetaActivity", "meta", Frequency.DAILY, "people")