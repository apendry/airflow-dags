from enum import Enum

class TaskIds(Enum):
    input = lambda ds: f"{ds.name.lower()}_inputs"
    output = lambda ds: f"{ds.name.lower()}_outputs"

class Keys(Enum):
    input = lambda ds: f"{ds.name.lower()}_success_paths"
    output = lambda ds: f"{ds.name.lower()}_output_paths"