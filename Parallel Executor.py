import pyodbc
import threading

# Function to connect to the SQL Server
def connect_to_db(server, database, username, password):
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
    )
    return conn

# Function to process a task
def process_task(task_id):
    print(f"Processing Task ID: {task_id}")
    # Add task processing logic here

# Function to fetch data from the database
def fetch_data(conn):
    cursor = conn.cursor()
    query = """
    SELECT p.process_id, p.process_name, sp.subprocess_id, sp.sequence as subprocess_sequence, 
           t.task_id, t.sequence as task_sequence
    FROM process p
    JOIN subprocess sp ON p.process_id = sp.process_id
    JOIN tasks t ON sp.subprocess_id = t.subprocess_id
    ORDER BY sp.sequence, t.sequence
    """
    cursor.execute(query)
    return cursor.fetchall()

# Function to handle task execution for a single subprocess
def handle_subprocess_tasks(tasks):
    # Group tasks by their sequence
    tasks_by_sequence = {}
    for task in tasks:
        task_sequence = task['task_sequence']
        if task_sequence not in tasks_by_sequence:
            tasks_by_sequence[task_sequence] = []
        tasks_by_sequence[task_sequence].append(task)

    # Process tasks sequence by sequence
    for sequence, task_group in sorted(tasks_by_sequence.items()):
        print(f"Processing task group with sequence: {sequence}")
        threads = []

        # Run tasks in parallel if they belong to the same sequence
        for task in task_group:
            thread = threading.Thread(target=process_task, args=(task['task_id'],))
            threads.append(thread)
            thread.start()

        # Wait for all tasks in this sequence to complete
        for thread in threads:
            thread.join()

# Function to process subprocesses, with parallelism based on subprocess sequence
def process_subprocesses(subprocesses):
    current_sequence = None
    subprocess_group = []

    for subprocess in subprocesses:
        subprocess_id = subprocess['subprocess_id']
        subprocess_sequence = subprocess['subprocess_sequence']
        tasks = subprocess['tasks']

        # Group subprocesses with the same sequence
        if current_sequence is None:
            current_sequence = subprocess_sequence

        if subprocess_sequence == current_sequence:
            subprocess_group.append((subprocess_id, tasks))
        else:
            # Run the grouped subprocesses in parallel
            run_subprocesses_in_parallel(subprocess_group)

            # Reset the group for the next sequence
            subprocess_group = [(subprocess_id, tasks)]
            current_sequence = subprocess_sequence

    # Process remaining subprocesses (if any)
    if subprocess_group:
        run_subprocesses_in_parallel(subprocess_group)

# Function to run a group of subprocesses in parallel
def run_subprocesses_in_parallel(subprocess_group):
    threads = []
    for subprocess_id, tasks in subprocess_group:
        print(f"Starting subprocess {subprocess_id} in parallel")
        thread = threading.Thread(target=handle_subprocess_tasks, args=(tasks,))
        threads.append(thread)
        thread.start()

    # Wait for all subprocess threads to complete
    for thread in threads:
        thread.join()

# Main function to fetch data and run processes and subprocesses
def process_all_tasks(conn):
    data = fetch_data(conn)

    # Group subprocesses by sequence
    subprocesses = []
    current_subprocess = None

    for row in data:
        subprocess_id = row.subprocess_id
        subprocess_sequence = row.subprocess_sequence
        task_info = {
            'task_id': row.task_id,
            'task_sequence': row.task_sequence
        }

        if current_subprocess is None or current_subprocess['subprocess_id'] != subprocess_id:
            if current_subprocess:
                subprocesses.append(current_subprocess)
            current_subprocess = {
                'subprocess_id': subprocess_id,
                'subprocess_sequence': subprocess_sequence,
                'tasks': []
            }

        current_subprocess['tasks'].append(task_info)

    if current_subprocess:
        subprocesses.append(current_subprocess)

    # Process subprocesses
    process_subprocesses(subprocesses)

if __name__ == "__main__":
    # DB connection parameters
    server = 'your_sql_server'
    database = 'your_database'
    username = 'your_username'
    password = 'your_password'
    
    # Establish connection
    conn = connect_to_db(server, database, username, password)

    # Process tasks
    process_all_tasks(conn)

    # Close connection
    conn.close()
