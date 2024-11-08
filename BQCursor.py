from google.cloud import bigquery
from typing import List, Optional, Tuple, Any, Iterator

class BigQueryCursor:
    def __init__(self, client: bigquery.Client):
        """Initialize the cursor with a BigQuery client."""
        self.client = client
        self.query_job = None
        self.row_iterator = None
        self._arraysize = 1  # Default batch size
        self._query = None
        self.description = None  # Metadata about columns
        self.rowcount = -1  # -1 indicates that the row count is unknown

    def execute(self, query: str, params: Optional[Tuple] = None) -> None:
        """Execute a query with optional parameters."""
        self._query = query
        if params:
            query = query % params  # Simple parameter substitution

        # Execute the query and wait for the results
        self.query_job = self.client.query(query)
        result = self.query_job.result()  # Wait for the query to complete
        self.row_iterator = iter(result)  # Create an iterator over the results

        # Get column metadata and row count
        self.description = self._get_description()
        self.rowcount = result.total_rows

    def executemany(self, query: str, param_list: List[Tuple]) -> None:
        """
        Execute the same query repeatedly with different parameter sets.
        
        For INSERTs or other DML operations that need to be repeated, each set
        of parameters will be applied to the query and executed.
        """
        self._query = query
        for params in param_list:
            formatted_query = query % params  # Simple parameter substitution
            self.client.query(formatted_query).result()  # Execute and wait for each query to complete

    def _get_description(self) -> List[Tuple[str, Any, None, None, None, None, None]]:
        """Generate column metadata for the `description` attribute."""
        if self.query_job:
            return [(field.name, field.field_type, None, None, None, None, None)
                    for field in self.query_job.schema]
        return []

    def fetchone(self) -> Optional[bigquery.Row]:
        """Fetch the next row or return None if no more rows are available."""
        return next(self.row_iterator, None) if self.row_iterator else None

    def fetchmany(self, size: Optional[int] = None) -> List[bigquery.Row]:
        """Fetch the next set of rows, limited by the given size or arraysize."""
        size = size or self._arraysize
        rows = []
        try:
            for _ in range(size):
                rows.append(next(self.row_iterator))
        except StopIteration:
            pass
        return rows

    def fetchall(self) -> List[bigquery.Row]:
        """Fetch all remaining rows."""
        return list(self.row_iterator) if self.row_iterator else []

    def close(self) -> None:
        """Close the cursor and release resources."""
        self.row_iterator = None
        print("Cursor closed.")

    def set_arraysize(self, size: int) -> None:
        """Set the default batch size for fetchmany()."""
        if size <= 0:
            raise ValueError("Arraysize must be a positive integer.")
        self._arraysize = size

    def reset(self) -> None:
        """Reset the cursor by re-running the query."""
        if self._query:
            self.execute(self._query)

    def __iter__(self) -> Iterator[bigquery.Row]:
        """Make the cursor iterable."""
        return iter(self.row_iterator)

    def __enter__(self):
        """Enable use with context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the cursor automatically when exiting the context."""
        self.close()


# Usage Example
client = bigquery.Client()

with BigQueryCursor(client) as cursor:
    # Executing a single query
    cursor.execute("SELECT name, age FROM `your-project.your_dataset.people` WHERE age > %d", (25,))
    row = cursor.fetchone()
    if row:
        print(f"Fetched one: {row}")

    # Using executemany() for batch inserts
    insert_query = "INSERT INTO `your-project.your_dataset.people` (name, age) VALUES ('%s', %d)"
    data = [
        ('John Doe', 30),
        ('Jane Smith', 28),
        ('Alice Johnson', 35)
    ]
    cursor.executemany(insert_query, data)
    print("Batch insert completed.")
