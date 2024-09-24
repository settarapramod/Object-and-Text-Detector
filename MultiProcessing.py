import multiprocessing
import time
from random import randint

# Define Subprocess and Task classes
class Task:
    def __init__(self, task_id, process_id, sequence):
        self.task_id = task_id
        self.process_id = process_id
        self.sequence = sequence

    def execute(self):
        print(f"Executing Task {self.task_id} from Process {self.process_id} with Sequence {self.sequence}")
        time.sleep(randint(1, 3))  # Simulate work with random delay
        print(f"Task {self.task_id} from Process {self.process_id} completed")


class Subprocess:
    def __init__(self, process_id, subprocess_id, sequence, task_list):
        self.process_id = process_id
        self.subprocess_id = subprocess_id
        self.sequence = sequence
        self.task_list = task_list

    def execute(self):
        print(f"Starting Subprocess {self.subprocess_id} from Process {self.process_id} with Sequence {self.sequence}")
        processes = []
        # Process tasks in parallel if they have the same sequence
        for task in self.task_list:
            p = multiprocessing.Process(target=task.execute)
            processes.append(p)
            p.start()
        for p in processes:
            p.join()  # Ensure all tasks are completed before moving to the next subprocess
        print(f"Subprocess {self.subprocess_id} completed")


# Simulate the data with subprocesses and tasks
subprocesses = [
    Subprocess(process_id=1, subprocess_id=101, sequence=1, task_list=[
        Task(task_id=1, process_id=1, sequence=1),
        Task(task_id=2, process_id=1, sequence=1)
    ]),
    Subprocess(process_id=1, subprocess_id=102, sequence=2, task_list=[
        Task(task_id=3, process_id=1, sequence=2),
        Task(task_id=4, process_id=1, sequence=2)
    ]),
    Subprocess(process_id=2, subprocess_id=103, sequence=1, task_list=[
        Task(task_id=5, process_id=2, sequence=1),
        Task(task_id=6, process_id=2, sequence=1)
    ]),
]

# Function to process subprocesses in sequence, with parallel tasks
def process_subprocesses(subprocesses):
    # Sort subprocesses by sequence to ensure sequential execution
    subprocesses.sort(key=lambda sp: sp.sequence)
    
    # Group subprocesses by sequence and process in parallel
    for sequence in sorted(set(sp.sequence for sp in subprocesses)):
        print(f"Processing Subprocesses with Sequence {sequence}")
        processes = []
        for sp in [sp for sp in subprocesses if sp.sequence == sequence]:
            p = multiprocessing.Process(target=sp.execute)
            processes.append(p)
            p.start()
        for p in processes:
            p.join()  # Ensure all subprocesses in this sequence are completed
        print(f"Completed Subprocesses with Sequence {sequence}\n")

# Execute the processing
if __name__ == "__main__":
    process_subprocesses(subprocesses)
