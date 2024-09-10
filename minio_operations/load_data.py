from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile


def load_data_to_minio(data, key_name):
    s3_hook = S3Hook(aws_conn_id='minio_connect')
    with NamedTemporaryFile(mode='w', suffix='.csv') as f:
        data.to_csv(f.name, index=False, header=False)
        f.flush()
        s3_hook.load_file(
            filename=f.name,
            key=f'processed/{key_name}.csv',
            bucket_name='datalake',
            replace=True
        )