Here’s a Python script to achieve this using the `pyodbc` library to connect to SQL Server and `threading` for parallel execution of tasks. The script fetches data from the `process`, `subprocess`, and `tasks` tables, then runs tasks in parallel if the sequences match and sequentially if not.

### Prerequisites:
1. Install `pyodbc` and `threading` libraries if you haven't already:
   ```bash
   pip install pyodbc
   ```

### Python Script:

```python
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

# Function to process tasks
def process_task(task_id):
    print(f"Processing Task ID: {task_id}")
    # You can add your actual task processing logic here

# Function to fetch data from the tables
def fetch_data(conn):
    cursor = conn.cursor()
    
    # Query to fetch process, subprocess and task data
    query = """
    SELECT p.process_id, p.process_name, sp.subprocess_id, sp.sequence as subprocess_sequence, 
           t.task_id, t.sequence as task_sequence
    FROM process p
    JOIN subprocess sp ON p.process_id = sp.process_id
    JOIN tasks t ON sp.subprocess_id = t.subprocess_id
    ORDER BY p.process_id, sp.sequence, t.sequence
    """
    
    cursor.execute(query)
    return cursor.fetchall()

# Function to process subprocesses
def process_subprocess(subprocess_tasks):
    threads = []
    
    # Grouping tasks by their sequence to run them in parallel or sequentially
    current_sequence = subprocess_tasks[0]['task_sequence']
    parallel_tasks = []
    
    for task in subprocess_tasks:
        if task['task_sequence'] == current_sequence:
            parallel_tasks.append(task)
        else:
            # Run the previous parallel tasks in threads
            run_parallel_tasks(parallel_tasks)
            current_sequence = task['task_sequence']
            parallel_tasks = [task]

    # Run any remaining parallel tasks
    if parallel_tasks:
        run_parallel_tasks(parallel_tasks)

# Function to run tasks in parallel
def run_parallel_tasks(tasks):
    threads = []
    for task in tasks:
        t = threading.Thread(target=process_task, args=(task['task_id'],))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()  # Ensure all tasks in the same sequence are completed

# Main function to process everything
def process_all_tasks(conn):
    data = fetch_data(conn)
    
    current_process = None
    current_subprocess = None
    subprocess_tasks = []

    for row in data:
        process_id = row.process_id
        subprocess_id = row.subprocess_id
        
        task_info = {
            'task_id': row.task_id,
            'task_sequence': row.task_sequence
        }
        
        if subprocess_id != current_subprocess:
            if subprocess_tasks:
                # Process the previous subprocess
                process_subprocess(subprocess_tasks)
            
            subprocess_tasks = [task_info]
            current_subprocess = subprocess_id
        else:
            subprocess_tasks.append(task_info)
    
    # Process any remaining subprocess
    if subprocess_tasks:
        process_subprocess(subprocess_tasks)

if __name__ == "__main__":
    # DB connection parameters
    server = 'your_sql_server'
    database = 'your_database'
    username = 'your_username'
    password = 'your_password'
    
    conn = connect_to_db(server, database, username, password)
    process_all_tasks(conn)
    conn.close()
```

### Explanation:
1. **Database Connection**: The `connect_to_db` function establishes a connection to the SQL Server using `pyodbc`.
   
2. **Fetching Data**: The `fetch_data` function retrieves data from the `process`, `subprocess`, and `tasks` tables in SQL Server.

3. **Processing Subprocess**: The `process_subprocess` function takes

a list of tasks under a subprocess and groups them based on their sequence. If multiple tasks share the same sequence, they are run in parallel using threads. Otherwise, tasks are executed sequentially.

4. **Running Tasks in Parallel**: The `run_parallel_tasks` function starts a new thread for each task with the same sequence and waits for all threads to finish before moving to the next sequence.

5. **Main Workflow**: The `process_all_tasks` function iterates over the fetched data. It keeps track of the current subprocess and its tasks, processing them as a batch when the subprocess changes or when the loop ends.

### Assumptions:
- Tasks with the same sequence within a subprocess run in parallel, while tasks with different sequences are processed sequentially.
- `process_task` is a placeholder function where you can implement the actual logic for processing a task.

### Customization:
- You may need to adjust the connection parameters and logic inside the `process_task` function to match your specific use case.
- For debugging or logging, you can add more `print` statements or use Python’s `logging` library.

Let me know if you need any further adjustments or clarifications!
