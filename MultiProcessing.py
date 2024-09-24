import multiprocessing
import time
from random import randint

# Define Task and Subprocess classes without execute method
class Task:
    def __init__(self, task_id, process_id, sequence):
        self.task_id = task_id
        self.process_id = process_id
        self.sequence = sequence


class Subprocess:
    def __init__(self, process_id, subprocess_id, sequence, task_list):
        self.process_id = process_id
        self.subprocess_id = subprocess_id
        self.sequence = sequence
        self.task_list = task_list


# Function to process a single task
def process_task(task):
    print(f"Executing Task {task.task_id} from Process {task.process_id} with Sequence {task.sequence}")
    time.sleep(randint(1, 3))  # Simulate work with random delay
    print(f"Task {task.task_id} from Process {task.process_id} completed")


# Function to process tasks in parallel if they share the same sequence
def process_tasks_parallel(task_list):
    task_list.sort(key=lambda t: t.sequence)  # Sort tasks by sequence for sequential processing
    for sequence in sorted(set(task.sequence for task in task_list)):
        print(f"Processing Tasks with Sequence {sequence} in Parallel")
        processes = []
        for task in [t for t in task_list if t.sequence == sequence]:
            p = multiprocessing.Process(target=process_task, args=(task,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()  # Ensure all tasks with the same sequence are completed
        print(f"Completed Tasks with Sequence {sequence}\n")


# Function to process a subprocess
def process_subprocess(subprocess):
    print(f"Starting Subprocess {subprocess.subprocess_id} from Process {subprocess.process_id} with Sequence {subprocess.sequence}")
    process_tasks_parallel(subprocess.task_list)
    print(f"Subprocess {subprocess.subprocess_id} completed\n")


# Function to process subprocesses, ensuring sequential execution of different sequences
def process_subprocesses(subprocesses):
    subprocesses.sort(key=lambda sp: sp.sequence)  # Sort subprocesses by sequence
    for sequence in sorted(set(sp.sequence for sp in subprocesses)):
        print(f"Processing Subprocesses with Sequence {sequence} in Parallel")
        processes = []
        for sp in [sp for sp in subprocesses if sp.sequence == sequence]:
            p = multiprocessing.Process(target=process_subprocess, args=(sp,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()  # Ensure all subprocesses with the same sequence are completed
        print(f"Completed Subprocesses with Sequence {sequence}\n")


# Simulate data with subprocesses and tasks
subprocesses = [
    Subprocess(process_id=1, subprocess_id=101, sequence=1, task_list=[
        Task(task_id=1, process_id=1, sequence=1),
        Task(task_id=2, process_id=1, sequence=1),
        Task(task_id=3, process_id=1, sequence=2),
    ]),
    Subprocess(process_id=1, subprocess_id=102, sequence=2, task_list=[
        Task(task_id=4, process_id=1, sequence=2),
        Task(task_id=5, process_id=1, sequence=2)
    ]),
    Subprocess(process_id=2, subprocess_id=103, sequence=1, task_list=[
        Task(task_id=6,
