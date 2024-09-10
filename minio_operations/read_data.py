from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def read_data_from_minio(file_name):
    s3_hook = S3Hook(aws_conn_id='minio_connect')
    file = s3_hook.read_key(
        key=f'raw/{file_name}.csv',
        bucket_name='datalake'
    )
    return file