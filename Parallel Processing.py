import apache_beam as beam
import time
from apache_beam.options.pipeline_options import PipelineOptions

# Define a simple Task class
class Task:
    def __init__(self, task_id, name):
        self.task_id = task_id
        self.name = name

# Simulate a process function with sleep
def process_task(task):
    print(f"Processing Task: {task.name}, Task ID: {task.task_id}")
    time.sleep(2)  # Simulate work with sleep
    print(f"Completed Task: {task.name}, Task ID: {task.task_id}")
    return task

# Create a list of Task objects
tasks = [Task(task_id=i, name=f"Task {i}") for i in range(5)]

# Set up Apache Beam pipeline options
pipeline_options = PipelineOptions()

# Define the Apache Beam pipeline
with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        | 'Create Task List' >> beam.Create(tasks)  # Create PCollection of Task objects
        | 'Process Tasks' >> beam.Map(process_task)  # Apply the process function to each task
    )
