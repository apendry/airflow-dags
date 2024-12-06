from ..datasets.dataset import Dataset
from ..enums.frequency import Frequency

example_dataset = Dataset("ExampleData", "example", Frequency.HOURLY)
example_metaset = Dataset("ExampleMeta", "meta", Frequency.DAILY)