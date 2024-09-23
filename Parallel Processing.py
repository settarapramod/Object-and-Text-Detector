import apache_beam as beam
import time
from apache_beam.options.pipeline_options import PipelineOptions

# Define a simple Task class with a sequence attribute
class Task:
    def __init__(self, task_id, name, sequence):
        self.task_id = task_id
        self.name = name
        self.sequence = sequence

# Simulate a process function with sleep
def process_task(task):
    print(f"Started Task: {task.name}, Task ID: {task.task_id}, Sequence: {task.sequence}")
    time.sleep(2)  # Simulate work with sleep
    print(f"Completed Task: {task.name}, Task ID: {task.task_id}, Sequence: {task.sequence}")
    return task

# Group tasks by sequence, ensuring parallelism for tasks within the same sequence
def process_sequence_group(task_group):
    sequence, tasks = task_group
    print(f"Processing sequence {sequence} with {len(tasks)} tasks")
    # Process tasks in parallel
    for task in tasks:
        process_task(task)
    return tasks

# Create a list of Task objects with different sequences
tasks = [
    Task(task_id=0, name="Task 0", sequence=1),
    Task(task_id=1, name="Task 1", sequence=2),
    Task(task_id=2, name="Task 2", sequence=2),
    Task(task_id=3, name="Task 3", sequence=3),
    Task(task_id=4, name="Task 4", sequence=3)
]

# Set up Apache Beam pipeline options to enable parallelism
pipeline_options = PipelineOptions([
    '--direct_num_workers=5',  # Set the number of parallel workers
])

# Define the Apache Beam pipeline
with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        | 'Create Task List' >> beam.Create(tasks)  # Create PCollection of Task objects
        | 'Group by Sequence' >> beam.GroupBy(lambda task: task.sequence)  # Group by sequence
        | 'Process Task Groups' >> beam.Map(process_sequence_group)  # Process each sequence group
    )
