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
        yield element  # Emit the first row

def run():
    # Set up pipeline options
    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = 'us-central1'
    gcp_options.temp_location = f'gs://{GCS_BUCKET}/temp/'

    # Initialize BigQuery client for running queries
    bq_client = bigquery.Client()

    with beam.Pipeline(options=options) as pipeline:
        # Step 1: Read Avro data from GCS
        avro_data = pipeline | 'Read Avro' >> beam.io.ReadFromAvro(AVRO_FILE_PATH)

        # Step 2: Extract a sample row and collect it as a side input
        sample_row_pcoll = (
            avro_data
            | 'Extract Sample Row' >> beam.ParDo(ExtractSampleRow())
            | 'Sample One Row' >> beam.combiners.Sample.FixedSizeGlobally(1)
        )

        # Step 3: Write to the BigQuery staging table using the dynamically generated schema
        def write_with_dynamic_schema(data, sample_row):
            schema = generate_bq_schema(sample_row[0])  # Extract schema from sample row
            return (
                data
                | 'Write to Staging BQ' >> WriteToBigQuery(
                    BQ_STAGING_TABLE,
                    schema=schema,
                    write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                )
            )

        avro_data | beam.FlatMap(write_with_dynamic_schema, beam.pvalue.AsSingleton(sample_row_pcoll))

    # Step 4: Load data from staging to reporting table
    query = f"""
    INSERT INTO `{BQ_REPORTING_TABLE}` 
    SELECT * FROM `{BQ_STAGING_TABLE}`
    """
    bq_client.query(query).result()
    print("Data successfully loaded from staging to reporting table.")

if __name__ == '__main__':
    run()
