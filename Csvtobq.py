import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class CSVToDict(beam.DoFn):
    def process(self, element):
        # Assuming the CSV columns are: id, name, age
        fields = element.split(',')
        return [{
            'id': int(fields[0]),
            'name': fields[1],
            'age': int(fields[2])
        }]

def run():
    # Set pipeline options
    options = PipelineOptions(
        project='your-gcp-project-id',
        region='your-region',
        runner='DataflowRunner',  # Use 'DirectRunner' for local testing
        temp_location='gs://your-bucket/temp',
        staging_location='gs://your-bucket/staging'
    )

    # GCS path and BigQuery table
    input_file = 'gs://your-bucket/path/to/input.csv'
    output_table = 'your-project:your_dataset.your_table'

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from GCS' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | 'Parse CSV to Dict' >> beam.ParDo(CSVToDict())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                output_table,
                schema='id:INTEGER,name:STRING,age:INTEGER',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
