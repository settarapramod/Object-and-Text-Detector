import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from google.cloud import bigquery

PROJECT_ID = 'your-project-id'
GCS_BUCKET = 'your-bucket-name'
AVRO_FILE_PATH = f'gs://{GCS_BUCKET}/path/to/your/data.avro'
BQ_STAGING_TABLE = 'your-project.your_dataset.staging_table'
BQ_REPORTING_TABLE = 'your-project.your_dataset.reporting_table'

def _get_bq_type(value):
    """Map Python types to BigQuery types."""
    if isinstance(value, int):
        return 'INTEGER'
    elif isinstance(value, float):
        return 'FLOAT'
    elif isinstance(value, bool):
        return 'BOOLEAN'
    elif isinstance(value, dict):
        return 'RECORD'
    else:
        return 'STRING'  # Default to STRING for unknown types

def generate_bq_schema(sample_row):
    """Generate BigQuery schema based on a sample row."""
    fields = [
        bigquery.SchemaField(key, _get_bq_type(value), mode='NULLABLE')
        for key, value in sample_row.items()
    ]
    return fields

class ExtractSampleRow(beam.DoFn):
    """Extract a sample row for schema generation."""
    def process(self, element):
        # Emit the first element to be used as a schema reference
        yield element
        return  # Stop after yielding the first row

def run():
    # Set up pipeline options
    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = 'us-central1'  # Set your region
    gcp_options.temp_location = f'gs://{GCS_BUCKET}/temp/'

    # Initialize BigQuery client for query execution
    bq_client = bigquery.Client()

    with beam.Pipeline(options=options) as pipeline:
        # Step 1: Read Avro data from GCS
        avro_data = pipeline | 'Read Avro' >> beam.io.ReadFromAvro(AVRO_FILE_PATH)

        # Step 2: Extract a sample row to generate the schema
        sample_row = (
            avro_data
            | 'Extract Sample Row' >> beam.ParDo(ExtractSampleRow())
            | 'Get First Row' >> beam.combiners.Sample.FixedSizeGlobally(1)
        )

        # Step 3: Write data to the BigQuery staging table
        (
            avro_data
            | 'Write to Staging BQ' >> WriteToBigQuery(
                BQ_STAGING_TABLE,
                schema=lambda _, row: generate_bq_schema(row[0]),
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

    # Step 4: Load data from staging to reporting table
    query = f"""
    INSERT INTO `{BQ_REPORTING_TABLE}` 
    SELECT * FROM `{BQ_STAGING_TABLE}`
    """
    bq_client.query(query).result()
    print("Data successfully loaded from staging to reporting table.")

if __name__ == '__main__':
    run()
