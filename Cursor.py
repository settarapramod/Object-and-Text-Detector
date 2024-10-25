from google.cloud import bigquery
from typing import Iterator, List, Optional

class BigQueryCursor:
    def __init__(self, query: str, client: bigquery.Client):
        """Initialize the cursor with the query and client."""
        self.query = query
        self.client = client
        self.query_job = self.client.query(query)
        self.row_iterator = iter(self.query_job)  # Create an iterator from the query result

    def fetchone(self) -> Optional[bigquery.Row]:
        """Fetch a single row or return None if no rows are left."""
        return next(self.row_iterator, None)

    def fetchmany(self, size: int) -> List[bigquery.Row]:
        """Fetch a specified number of rows."""
        rows = []
        try:
            for _ in range(size):
                rows.append(next(self.row_iterator))
        except StopIteration:
            pass  # Stop if no more rows are available
        return rows

    def fetchall(self) -> List[bigquery.Row]:
        """Fetch all remaining rows."""
        return list(self.row_iterator)

    def reset(self):
        """Reset the cursor to re-run the query and restart the iterator."""
        self.query_job = self.client.query(self.query)
        self.row_iterator = iter(self.query_job)  # Re-create the iterator

    def close(self):
        """Close the cursor and release resources."""
        self.row_iterator = iter([])  # Invalidate the iterator to free memory
        print("Cursor closed.")

# Usage Example
client = bigquery.Client()

# Initialize a cursor with a sample query
cursor = BigQueryCursor("SELECT name, age FROM `your-project.your_dataset.people`", client)

# Fetch one row
row = cursor.fetchone()
if row:
    print(f"Fetched one: {row}")

# Fetch many rows (batch size 3)
batch = cursor.fetchmany(3)
print(f"Fetched batch: {batch}")

# Fetch all remaining rows
all_rows = cursor.fetchall()
print(f"Fetched all: {all_rows}")

# Reset and re-run the query
cursor.reset()

# Close the cursor
cursor.close()
