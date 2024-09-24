import concurrent.futures
import time
from collections import defaultdict

# Define the Subprocess class
class SubProcess:
    def __init__(self, process_id, subprocess_id, sequence, tasks):
        self.process_id = process_id
        self.subprocess_id = subprocess_id
        self.sequence = sequence
        self.tasks = tasks

# Define the Task class
class Task:
    def __init__(self, task_id, subprocess_id, process_id, sequence):
        self.task_id = task_id
        self.subprocess_id = subprocess_id
        self.process_id = process_id
        self.sequence = sequence

# Simulate task processing
def process_task(task):
    print(f"Processing Task {task.task_id} from Subprocess {task.subprocess_id} (Sequence {task.sequence})")
    time.sleep(2)  # Simulate task processing time
    print(f"Task {task.task_id} from Subprocess {task.subprocess_id} completed.")

# Simulate subprocess processing
def process_subprocess(subprocess):
    print(f"Processing Subprocess {subprocess.subprocess_id} (Sequence {subprocess.sequence})")
    
    # Group tasks by their sequence for parallel processing
    tasks_by_sequence = defaultdict(list)
    for task in subprocess.tasks:
        tasks_by_sequence[task.sequence].append(task)
    
    # Process tasks in sequence order
    for sequence in sorted(tasks_by_sequence):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(process_task, tasks_by_sequence[sequence])
    
    print(f"Subprocess {subprocess.subprocess_id} completed.")

# Function to process all subprocesses
def process_subprocesses(subprocesses):
    # Group subprocesses by their sequence for parallel processing
    subprocess_by_sequence = defaultdict(list)
    for subprocess in subprocesses:
        subprocess_by_sequence[subprocess.sequence].append(subprocess)
    
    # Process subprocesses in sequence order
    for sequence in sorted(subprocess_by_sequence):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(process_subprocess, subprocess_by_sequence[sequence])

# Sample Data Creation
task_list1 = [Task(task_id=1, subprocess_id=101, process_id=1, sequence=1),
              Task(task_id=2, subprocess_id=101, process_id=1, sequence=2),
              Task(task_id=3, subprocess_id=101, process_id=1, sequence=2)]

task_list2 = [Task(task_id=4, subprocess_id=102, process_id=1, sequence=1),
              Task(task_id=5, subprocess_id=102, process_id=1, sequence=1),
              Task(task_id=6, subprocess_id=102, process_id=1, sequence=2)]

subprocess1 = SubProcess(process_id=1, subprocess_id=101, sequence=1, tasks=task_list1)
subprocess2 = SubProcess(process_id=1, subprocess_id=102, sequence=2, tasks=task_list2)

# List of subprocesses to process
subprocesses = [subprocess1, subprocess2]

# Process all subprocesses
process_subprocesses(subprocesses)
