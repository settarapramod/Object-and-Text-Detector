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

        # Execute the query
        self.query_job = self.client.query(query)
        self.row_iterator = iter(self.query_job)
        self.description = self._get_description()
        self.rowcount = self.query_job.total_rows  # Total rows in the result

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

# Use the cursor in a context manager
with BigQueryCursor(client) as cursor:
    # Execute a query with parameter substitution
    cursor.execute("SELECT name, age FROM `your-project.your_dataset.people` WHERE age > %d", (25,))

    # Fetch one row
    row = cursor.fetchone()
    if row:
        print(f"Fetched one: {row}")

    # Fetch many rows (batch size 3)
    cursor.set_arraysize(3)
    batch = cursor.fetchmany()
    print(f"Fetched batch: {batch}")

    # Fetch all remaining rows
    all_rows = cursor.fetchall()
    print(f"Fetched all: {all_rows}")

    # Access metadata about the columns
    print(f"Column Description: {cursor.description}")

    # Access the total row count
    print(f"Total Rows: {cursor.rowcount}")
