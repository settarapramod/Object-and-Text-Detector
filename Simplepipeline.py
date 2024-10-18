import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class CountWords(beam.DoFn):
    def process(self, element):
        word = element['word']
        yield (word, 1)

def run():
    # Define pipeline options for Dataflow execution
    options = PipelineOptions(
        runner='DataflowRunner',
        project='your-gcp-project-id',
        region='your-region',
        temp_location='gs://your-bucket/temp/',
        job_name='bq-beam-job'
    )

    # Define the schema for the output table
    output_schema = {
        "fields": [
            {"name": "word", "type": "STRING", "mode": "REQUIRED"},
            {"name": "count", "type": "INTEGER", "mode": "REQUIRED"}
        ]
    }

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromBigQuery' >> ReadFromBigQuery(
                query='SELECT word FROM `bigquery-public-data.samples.shakespeare`',
                use_standard_sql=True
            )
            | 'CountWords' >> beam.ParDo(CountWords())
            | 'SumCounts' >> beam.CombinePerKey(sum)
            | 'FormatForBQ' >> beam.Map(lambda x: {'word': x[0], 'count': x[1]})
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table='your-gcp-project-id:my_dataset.word_counts',
                schema=output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
