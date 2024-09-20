import apache_beam as beam

# Define the Task class
class Task:
    def __init__(self, task_id, subprocess_id, sequence):
        self.task_id = task_id
        self.subprocess_id = subprocess_id
        self.sequence = sequence

    def __repr__(self):
        return f"Task(task_id={self.task_id}, subprocess_id={self.subprocess_id}, sequence={self.sequence})"

# Define the Subprocess class
class Subprocess:
    def __init__(self, subprocess_id, process_id, sequence, tasks):
        self.subprocess_id = subprocess_id
        self.process_id = process_id
        self.sequence = sequence
        self.tasks = tasks

    def __repr__(self):
        return f"Subprocess(subprocess_id={self.subprocess_id}, process_id={self.process_id}, sequence={self.sequence}, tasks={self.tasks})"

# Function to process a single task
def process_task(task):
    print(f"Processing Task ID: {task.task_id} in Subprocess ID: {task.subprocess_id} with sequence {task.sequence}")
    return task

# Function to process a single subprocess by breaking down tasks based on their sequence
class ProcessSubprocess(beam.DoFn):
    def process(self, subprocess):
        # Group tasks by their sequence number
        task_groups = {}
        for task in subprocess.tasks:
            if task.sequence not in task_groups:
                task_groups[task.sequence] = []
            task_groups[task.sequence].append(task)
        
        # Process tasks with the same sequence in parallel
        for seq in sorted(task_groups.keys()):
            # Emit each task for processing
            for task in task_groups[seq]:
                yield task

# Function to create and run the Beam pipeline
def run_pipeline(subprocess_list):
    with beam.Pipeline() as pipeline:
        # Create PCollection from the list of subprocesses
        subprocesses = pipeline | 'Create Subprocesses' >> beam.Create(subprocess_list)
        
        # Process each subprocess and the tasks within them
        (subprocesses
         | 'Process Subprocesses' >> beam.ParDo(ProcessSubprocess())
         | 'Process Each Task' >> beam.Map(process_task)
        )

# Example usage
if __name__ == "__main__":
    # Define example tasks and subprocesses
    tasks1 = [Task(task_id=1, subprocess_id=1, sequence=1), Task(task_id=2, subprocess_id=1, sequence=2)]
    tasks2 = [Task(task_id=3, subprocess_id=2, sequence=1), Task(task_id=4, subprocess_id=2, sequence=2)]
    subprocess1 = Subprocess(subprocess_id=1, process_id=1, sequence=1, tasks=tasks1)
    subprocess2 = Subprocess(subprocess_id=2, process_id=1, sequence=2, tasks=tasks2)

    # Create a list of subprocesses
    subprocess_list = [subprocess1, subprocess2]

    # Run the Apache Beam pipeline
    run_pipeline(subprocess_list)
