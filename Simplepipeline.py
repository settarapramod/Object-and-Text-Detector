import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class ProcessData(beam.DoFn):
    def process(self, element):
        # Split CSV rows into fields
        id, name, age = element.split(',')
        yield {'id': int(id), 'name': name, 'age': int(age) + 1}  # Increment age by 1

def run():
    # Set pipeline options for Dataflow runner
    options = PipelineOptions(
        runner='DataflowRunner',
        project='your-gcp-project-id',
        region='your-region',
        temp_location='gs://your-bucket/temp/',
        job_name='sample-beam-job'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromGCS' >> beam.io.ReadFromText('gs://your-bucket/sample.csv', skip_header_lines=1)
            | 'ProcessData' >> beam.ParDo(ProcessData())
            | 'WriteToGCS' >> beam.io.WriteToText('gs://your-bucket/output/result', file_name_suffix='.json')
        )

if __name__ == '__main__':
    run()
