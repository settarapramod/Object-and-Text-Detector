import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.gcp.gcsio import WriteToText

class FormatToCSV(beam.DoFn):
    def process(self, row):
        # Convert dictionary row into a CSV string
        yield ','.join(map(str, row.values()))

def run():
    project = 'your-gcp-project-id'
    bucket = 'your-gcs-bucket-name'
    bq_table = 'your-dataset.your-table'
    output_path = f'gs://{bucket}/output.csv'

    # Beam pipeline options
    options = PipelineOptions(
        project=project,
        temp_location=f'gs://{bucket}/temp',
        region='your-gcp-region',  # Example: 'us-central1'
        runner='DataflowRunner'  # Use 'DirectRunner' for local execution
    )

    # Create the Beam pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromBigQuery' >> ReadFromBigQuery(table=bq_table)
            | 'FormatToCSV' >> beam.ParDo(FormatToCSV())
            | 'WriteToGCS' >> WriteToText(output_path, file_name_suffix='.csv', shard_name_template='')
        )

if __name__ == '__main__':
    run()
