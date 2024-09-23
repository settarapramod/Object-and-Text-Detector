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

# Partition function to split tasks by sequence
def partition_by_sequence(task, num_partitions):
    return task.sequence - 1  # Partition by sequence (assuming sequences are 1-based)

# Create a list of Task objects with different sequences
tasks = [
    Task(task_id=0, name="Task 0", sequence=1),
    Task(task_id=1, name="Task 1", sequence=2),
    Task(task_id=2, name="Task 2", sequence=2),
    Task(task_id=3, name="Task 3", sequence=3),
    Task(task_id=4, name="Task 4", sequence=3)
]

# Set up Apache Beam pipeline options to enable multi-threading or multi-processing
pipeline_options = PipelineOptions([
    '--direct_num_workers=5',  # Set the number of parallel workers
    '--direct_running_mode=multi_threading'  # Enable multi-threading in DirectRunner
])

# Define the Apache Beam pipeline
with beam.Pipeline(options=pipeline_options) as p:
    # Create the PCollection of tasks
    task_pcollection = p | 'Create Task List' >> beam.Create(tasks)
    
    # Partition the tasks by sequence number
    partitions = task_pcollection | 'Partition by Sequence' >> beam.Partition(partition_by_sequence, 3)
    
    # Process each partition (sequence) in order, but tasks within the same sequence in parallel
    for i in range(3):  # Assuming we know the number of sequences
        (
            partitions[i]  # Get the partition corresponding to the sequence number
            | f'Process Sequence {i+1}' >> beam.Map(process_task)
        )
