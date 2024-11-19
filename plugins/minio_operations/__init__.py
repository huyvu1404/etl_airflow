from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile


def load_data_to_minio(data, key_name):
    """ Store a pandas DataFrame as CSV file into Minio bucket.

    Args:
        data (pandas.DataFrame): DataFrame to be stored.
        key_name (str): Name of file in the Minio bucket.
    """
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
        
def read_data_from_minio(file_name):
    """ Read a CSV file from Minio bucket.

    Args:
        file_name (str): Name of the file (without extension) stored in Minio bucket.

    Returns:
        str: The content of the file as a string.
    """
    s3_hook = S3Hook(aws_conn_id='minio_connect')
    file = s3_hook.read_key(
        key=f'raw/{file_name}.csv',
        bucket_name='datalake'
    )
    return file