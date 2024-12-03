from src.common.datasets.dataset import Dataset
from src.common.enums.frequency import Frequency

example_dataset = Dataset("ExampleData", "example", Frequency.HOURLY)
example_metaset = Dataset("ExampleMeta", "example", Frequency.DAILY, suffix_dir="meta")