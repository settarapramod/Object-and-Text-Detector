import time
from concurrent.futures import ProcessPoolExecutor, as_completed

# Define a simple Task class with a sequence attribute
class Task:
    def __init__(self, task_id, name, sequence):
        self.task_id = task_id
        self.name = name
        self.sequence = sequence

    def __repr__(self):
        return f"Task(name={self.name}, task_id={self.task_id}, sequence={self.sequence})"

# Simulate a process function with sleep
def process_task(task):
    print(f"Started Task: {task.name}, Task ID: {task.task_id}, Sequence: {task.sequence}")
    time.sleep(2)  # Simulate work with sleep
    print(f"Completed Task: {task.name}, Task ID: {task.task_id}, Sequence: {task.sequence}")
    return task

# Function to process tasks in parallel within a sequence
def process_tasks_in_parallel(tasks, max_workers):
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_task, task) for task in tasks]
        for future in as_completed(futures):
            future.result()  # Ensure all tasks complete before proceeding

# Function to enforce sequential execution by sequence
def process_sequence_group(task_group, max_workers):
    sequence, tasks = task_group
    print(f"\nProcessing tasks for sequence {sequence}\n")
    process_tasks_in_parallel(tasks, max_workers)  # Process tasks in parallel within the same sequence

# Create a list of Task objects with different sequences
tasks = [
    Task(task_id=0, name="Task 0", sequence=1),
    Task(task_id=1, name="Task 1", sequence=2),
    Task(task_id=2, name="Task 2", sequence=2),
    Task(task_id=3, name="Task 3", sequence=3),
    Task(task_id=4, name="Task 4", sequence=3)
]

# Function to run the entire process
def run_tasks_in_sequence(tasks, max_workers):
    # Group tasks by sequence
    grouped_tasks = {}
    for task in tasks:
        if task.sequence not in grouped_tasks:
            grouped_tasks[task.sequence] = []
        grouped_tasks[task.sequence].append(task)

    # Sort by sequence to process in ascending order
    for sequence in sorted(grouped_tasks.keys()):
        process_sequence_group((sequence, grouped_tasks[sequence]), max_workers)

# Set the number of parallel workers (control the number of processes)
max_workers = 2  # You can change this based on your machine's capability

# Run the tasks
run_tasks_in_sequence(tasks, max_workers)
