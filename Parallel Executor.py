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
def handle_subprocess_tasks(subprocess_id, tasks):
    print(f"Starting subprocess {subprocess_id}")
    threads = []

    # Run tasks in parallel if they have the same sequence
    for task in tasks:
        thread = threading.Thread(target=process_task, args=(task['task_id'],))
        threads.append(thread)
        thread.start()

    # Wait for all threads (tasks) to complete
    for thread in threads:
        thread.join()

# Function to process subprocesses in parallel
def process_subprocesses(subprocesses):
    threads = []

    # Run each subprocess in parallel if it has the same sequence
    for subprocess_id, tasks in subprocesses.items():
        thread = threading.Thread(target=handle_subprocess_tasks, args=(subprocess_id, tasks))
        threads.append(thread)
        thread.start()

    # Wait for all subprocess threads to complete
    for thread in threads:
        thread.join()

# Main function to fetch data and run processes and subprocesses
def process_all_tasks(conn):
    data = fetch_data(conn)
    
    # Group subprocesses by their sequence
    subprocesses = {}
    
    for row in data:
        subprocess_id = row.subprocess_id
        subprocess_sequence = row.subprocess_sequence
        task_info = {
            'task_id': row.task_id,
            'task_sequence': row.task_sequence
        }
        
        if subprocess_id not in subprocesses:
            subprocesses[subprocess_id] = []
        
        subprocesses[subprocess_id].append(task_info)

    # Run all subprocesses
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
