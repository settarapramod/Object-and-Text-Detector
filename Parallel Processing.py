import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from collections import defaultdict
import time
import threading

# Define the classes for Process, Subprocess, and Task
class Task:
    def __init__(self, task_id, subprocess_id, sequence):
        self.task_id = task_id
        self.subprocess_id = subprocess_id
        self.sequence = sequence

    def __repr__(self):
        return f"Task({self.task_id}, Subprocess: {self.subprocess_id}, Seq: {self.sequence})"

class Subprocess:
    def __init__(self, subprocess_id, process_id, sequence, tasks):
        self.subprocess_id = subprocess_id
        self.process_id = process_id
        self.sequence = sequence
        self.tasks = tasks  # List of Task objects

    def __repr__(self):
        return f"Subprocess({self.subprocess_id}, Seq: {self.sequence})"

class Process:
    def __init__(self, process_id, subprocesses):
        self.process_id = process_id
        self.subprocesses = subprocesses  # List of Subprocess objects

    def __repr__(self):
        return f"Process({self.process_id})"

# Function to simulate task processing with sleep
def process_task(task):
    print(f"Starting Task {task.task_id} in Subprocess {task.subprocess_id} with Sequence {task.sequence}")
    time.sleep(2)  # Simulate work being done
    print(f"Completed Task {task.task_id} in Subprocess {task.subprocess_id} with Sequence {task.sequence}")
    return f"Processed {task}"

# Beam Transform to group subprocesses by their sequence
class GroupBySubprocessSequence(beam.DoFn):
    def process(self, process):
        sequence_map = defaultdict(list)
        for subprocess in process.subprocesses:
            sequence_map[subprocess.sequence].append(subprocess)
        for sequence, subprocesses in sequence_map.items():
            yield (sequence, subprocesses)

# Beam Transform to group tasks by their sequence within each subprocess
class GroupTasksBySequence(beam.DoFn):
    def process(self, subprocess):
        sequence_map = defaultdict(list)
        for task in subprocess.tasks:
            sequence_map[task.sequence].append(task)
        for sequence, tasks in sequence_map.items():
            yield (subprocess, sequence, tasks)

# Pipeline logic
def run_pipeline(process):
    options = PipelineOptions()

    # Create the Beam pipeline
    with beam.Pipeline(options=options) as p:
        
        # Step 1: Create a PCollection of processes
        process_pcoll = p | 'Create Process' >> beam.Create([process])

        # Step 2: Group subprocesses by their sequence and flatten them
        subprocess_by_sequence = (
            process_pcoll
            | 'Group Subprocesses by Sequence' >> beam.ParDo(GroupBySubprocessSequence())
        )

        # Step 3: Flatten the list of subprocesses and process each subprocess sequence in parallel
        subprocess_flattened = (
            subprocess_by_sequence
            | 'Flatten Subprocesses' >> beam.FlatMap(lambda x: x[1])  # Flatten to get individual subprocesses
        )

        # Step 4: Group tasks within each subprocess by their sequence and process them in parallel
        tasks_by_sequence = (
            subprocess_flattened
            | 'Group Tasks by Sequence' >> beam.ParDo(GroupTasksBySequence())
        )

        # Step 5: Process tasks in parallel by their sequence
        def process_subprocess_and_tasks(subprocess_sequence_tasks):
            subprocess, sequence, tasks = subprocess_sequence_tasks
            print(f"Processing Subprocess {subprocess.subprocess_id} with Sequence {subprocess.sequence}")

            # Process all tasks in this sequence in parallel using threading
            threads = []
            for task in tasks:
                thread = threading.Thread(target=process_task, args=(task,))
                thread.start()
                threads.append(thread)

            # Wait for all threads to finish
            for thread in threads:
                thread.join()

            print(f"Completed Subprocess {subprocess.subprocess_id} with Sequence {subprocess.sequence}")

        # Process each subprocess and its tasks in parallel
        processed_tasks = (
            tasks_by_sequence
            | 'Process Subprocess and Tasks' >> beam.Map(process_subprocess_and_tasks)
        )

        # Output the processed tasks (for debugging purposes)
        processed_tasks | 'Print Results' >> beam.Map(print)

# Sample data for testing
def create_sample_data():
    tasks1 = [Task(1, 1, 1), Task(2, 1, 2), Task(3, 1, 2), Task(4, 1, 3)]
    tasks2 = [Task(5, 2, 1), Task(6, 2, 2), Task(7, 2, 3)]
    tasks3 = [Task(8, 3, 1), Task(9, 3, 2), Task(10, 3, 2)]
    tasks4 = [Task(11, 4, 1), Task(12, 4, 2)]
    tasks5 = [Task(13, 5, 1), Task(14, 5, 1)]

    subprocess1 = Subprocess(1, 1, 1, tasks1)
    subprocess2 = Subprocess(2, 1, 2, tasks2)
    subprocess3 = Subprocess(3, 1, 2, tasks3)
    subprocess4 = Subprocess(4, 1, 3, tasks4)
    subprocess5 = Subprocess(5, 1, 3, tasks5)

    process = Process(1, [subprocess1, subprocess2, subprocess3, subprocess4, subprocess5])
    return process

# Run the pipeline with the sample data
if __name__ == "__main__":
    sample_process = create_sample_data()
    run_pipeline(sample_process)
