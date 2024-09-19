import pyodbc
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Database connection
def connect_to_db(server, database, username, password):
    return pyodbc.connect(
        'DRIVER={SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
    )

# Class definitions
class Process:
    def __init__(self, process_id, process_name):
        self.process_id = process_id
        self.process_name = process_name

class Subprocess:
    def __init__(self, subprocess_id, process_id, sequence):
        self.subprocess_id = subprocess_id
        self.process_id = process_id
        self.sequence = sequence
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)

class Task:
    def __init__(self, task_id, subprocess_id, sequence):
        self.task_id = task_id
        self.subprocess_id = subprocess_id
        self.sequence = sequence

    def process(self):
        print(f"Processing Task ID: {self.task_id} from Subprocess {self.subprocess_id}")

# Fetch subprocesses by process_id
def fetch_subprocesses(conn, process_id):
    cursor = conn.cursor()
    query = """
        SELECT subprocess_id, process_id, sequence
        FROM subprocess
        WHERE process_id = ?
    """
    cursor.execute(query, (process_id,))
    result = cursor.fetchall()
    
    return [Subprocess(row.subprocess_id, row.process_id, row.sequence) for row in result]

# Fetch tasks by subprocess_id
def fetch_tasks(conn, subprocess_id):
    cursor = conn.cursor()
    query = """
        SELECT task_id, subprocess_id, sequence
        FROM tasks
        WHERE subprocess_id = ?
    """
    cursor.execute(query, (subprocess_id,))
    result = cursor.fetchall()
    
    return [Task(row.task_id, row.subprocess_id, row.sequence) for row in result]

# Process a subprocess
def process_subprocess(subprocess):
    print(f"Processing Subprocess ID: {subprocess.subprocess_id} with sequence: {subprocess.sequence}")

    # Group tasks by sequence and process them in parallel
    tasks_by_sequence = {}
    for task in subprocess.tasks:
        if task.sequence not in tasks_by_sequence:
            tasks_by_sequence[task.sequence] = []
        tasks_by_sequence[task.sequence].append(task)

    for sequence, tasks in sorted(tasks_by_sequence.items()):
        print(f"Processing task group with sequence: {sequence}")
        for task in tasks:
            task.process()

# Beam pipeline for parallel processing of subprocesses
def run_pipeline(process_id):
    pipeline_options = PipelineOptions()

    # Create a connection to the database
    conn = connect_to_db('your_sql_server', 'your_database', 'your_username', 'your_password')

    # Fetch subprocesses based on process_id
    subprocesses = fetch_subprocesses(conn, process_id)

    # For each subprocess, fetch corresponding tasks
    for subprocess in subprocesses:
        subprocess.tasks = fetch_tasks(conn, subprocess.subprocess_id)

    # Close the database connection
    conn.close()

    # Apache Beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Create a PCollection of subprocesses
        subprocesses_pcollection = (
            p
            | 'Create subprocesses' >> beam.Create(subprocesses)
            # Group subprocesses by sequence
            | 'Group subprocesses by sequence' >> beam.GroupByKey(lambda sp: sp.sequence)
        )

        # Process each subprocess group in parallel
        (
            subprocesses_pcollection
            | 'Process Subprocesses' >> beam.Map(lambda group: process_subprocess(group[1]))
        )

if __name__ == "__main__":
    # Process ID to filter subprocesses
    process_id = input("Enter the process ID: ")

    # Run the pipeline
    run_pipeline(process_id)
            
