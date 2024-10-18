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

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowPythonOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 10, 18),
    'retries': 1,
}

with models.DAG(
    'beam_bq_pipeline_dag',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    run_beam_bq_pipeline = DataflowPythonOperator(
        task_id='run_beam_bq_pipeline',
        py_file='gs://your-composer-bucket/dags/beam_bq_pipeline.py',
        job_name='composer-bq-beam-job',
        project_id='your-gcp-project-id',
        location='your-region',
        dataflow_default_options={
            'tempLocation': 'gs://your-bucket/temp/',
        },
    )
